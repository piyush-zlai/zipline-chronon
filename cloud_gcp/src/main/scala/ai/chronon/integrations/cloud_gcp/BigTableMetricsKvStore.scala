package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.KVStore
import ai.chronon.online.metrics.Metrics
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.{CreateTableRequest, GCRules}
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Filters, Query, RowMutation, TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.{Logger, LoggerFactory}
import org.threeten.bp.Duration

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, YearMonth, ZoneOffset}
import scala.collection.concurrent.TrieMap
import scala.collection.{Seq, mutable}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** BigTable-based KV store implementation specifically for data quality metrics.
  *
  * Key scheme: <node_name>#<yyyy-MM>#<metric_type>
  * Value scheme: JSON with structure:
  * {
  *   "<metric_type>": [1,2,3,4, ...],
  *   "row_count": 200,
  *   "timestamp": <day_in_ms>
  * }
  *
  * Each value cell contains a snapshot for a specific day, with the cell timestamp
  * being the day timestamp in milliseconds.
  */
class BigTableMetricsKvStore(dataClient: BigtableDataClient,
                             tableBaseName: String,
                             maybeAdminClient: Option[BigtableTableAdminClient] = None,
                             conf: Map[String, String] = Map.empty)
    extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import BigTableMetricsKvStore._

  // Keep data around for 90 days for data quality metrics
  private val DataTTL = Duration.ofDays(90)

  // Cap the maximum number of cells we store per row
  private val MaxCellCount = 1000

  // GC rules for data quality metrics table
  private val DefaultGcRules =
    GCRules.GCRULES.union().rule(GCRules.GCRULES.maxAge(DataTTL)).rule(GCRules.GCRULES.maxVersions(MaxCellCount))

  protected val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.KVStore).withSuffix("data_quality_metrics")

  protected val tableToContext = new TrieMap[String, Metrics.Context]()

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    maybeAdminClient
      .map { adminClient =>
        try {
          val tableName = mapDatasetToTable(dataset)

          if (!adminClient.exists(tableName.toString)) {
            val createTableRequest =
              CreateTableRequest.of(tableName.toString).addFamily(ColumnFamilyString, DefaultGcRules)
            val table = adminClient.createTable(createTableRequest)
            logger.info(s"Created data quality metrics table: $table")
            metricsContext.increment("create.successes")
          } else {
            logger.info(s"Data quality metrics table $tableName already exists")
          }
        } catch {
          case e: Exception =>
            logger.error("Error creating data quality metrics table", e)
            metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        }
      }
      .orElse(throw new IllegalStateException("Missing BigTable admin client. Is the ENABLE_UPLOAD_CLIENTS flag set?"))
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} data quality metrics requests")

    val requestsByDataset = requests.groupBy(_.dataset)
    val datasetFutures = readRowsMultiGet(requestsByDataset)
    Future.sequence(datasetFutures).map(_.flatten)
  }

  private def readRowsMultiGet(
      requestsByDataset: Map[String, Seq[KVStore.GetRequest]]): Seq[Future[Seq[KVStore.GetResponse]]] = {
    requestsByDataset.map { case (dataset, datasetRequests) =>
      val targetId = mapDatasetToTable(dataset)
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        targetId.toString,
        metricsContext.copy(dataset = targetId.toString)
      )

      val query = Query
        .create(targetId)
        .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))

      val (finalQuery, requestsWithRowKeys) =
        datasetRequests.foldLeft((query, List.empty[(KVStore.GetRequest, mutable.ArrayBuffer[ByteString])])) {
          case ((currentQuery, acc), request) =>
            val rowKeys = new mutable.ArrayBuffer[ByteString]()

            // Data quality metrics are always time series
            val startTs = request.startTsMillis.getOrElse(System.currentTimeMillis() - 1.day.toMillis)
            val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())
            val (updatedQuery, addedRowKeys) =
              setQueryTimeSeriesFilters(currentQuery, startTs, endTime, request.keyBytes)
            rowKeys ++= addedRowKeys

            (updatedQuery, acc :+ (request, rowKeys))
        }

      val startTs = System.currentTimeMillis()
      val apiFuture = dataClient.readRowsCallable().all().futureCall(finalQuery)
      val scalaResultFuture = BigTableKVStore.googleFutureToScalaFuture(apiFuture)

      scalaResultFuture
        .map { rows =>
          datasetMetricsContext.distribution("multiGet.latency", System.currentTimeMillis() - startTs)
          datasetMetricsContext.increment("multiGet.successes")

          val rowKeyToRowMap = rows.asScala.map(row => row.getKey() -> row).toMap

          requestsWithRowKeys.map { case (request, rowKeys) =>
            val timedValues = rowKeys.flatMap { rowKey =>
              rowKeyToRowMap.get(rowKey).toSeq.flatMap { row =>
                // Get all cells from all qualifiers (metric names) in the column family
                row.getCells(ColumnFamilyString).asScala.map { cell =>
                  KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
                }
              }
            }
            KVStore.GetResponse(request, Success(timedValues))
          }
        }
        .recover { case e: Exception =>
          logger.error("Error getting data quality metrics values", e)
          datasetMetricsContext.increment("multiGet.bigtable_errors", Map("exception" -> e.getClass.getName))
          datasetRequests.map { request =>
            KVStore.GetResponse(request, Failure(e))
          }
        }
    }.toSeq
  }

  private def setQueryTimeSeriesFilters(query: Query,
                                        startTs: Long,
                                        endTs: Long,
                                        keyBytes: Seq[Byte]): (Query, Iterable[ByteString]) = {

    val keyString = new String(keyBytes.toArray, StandardCharsets.UTF_8)

    // Special case for schema rows - they don't have YYYY-MM suffix
    if (keyString.endsWith("#schema")) {
      val schemaRowKey = buildDataQualityRowKey(keyBytes, None)
      val schemaRowKeyByteString = ByteString.copyFrom(schemaRowKey)
      query.rowKey(schemaRowKeyByteString)

      // For schema rows, get the latest timestamped cell without time range filters
      // This allows us to get the most recent schema regardless of the query time range
      (query, List(schemaRowKeyByteString))
    } else {
      // Generate row keys for each year-month in the range for regular metric rows
      val yearMonths = generateYearMonthRange(startTs, endTs)

      val rowKeyByteStrings = yearMonths.map { ymTimestamp =>
        val rowKey = buildDataQualityRowKey(keyBytes, Some(ymTimestamp))
        val rowKeyByteString = ByteString.copyFrom(rowKey)
        query.rowKey(rowKeyByteString)
        rowKeyByteString
      }

      val modifiedQuery =
        query.filter(Filters.FILTERS.timestamp().range().startClosed(startTs * 1000).endClosed(endTs * 1000))
      (modifiedQuery, rowKeyByteStrings)
    }
  }

  private def generateYearMonthRange(startTs: Long, endTs: Long): List[Long] = {
    val startZoned = Instant.ofEpochMilli(startTs).atZone(ZoneOffset.UTC)
    val endZoned = Instant.ofEpochMilli(endTs).atZone(ZoneOffset.UTC)

    val start = YearMonth.of(startZoned.getYear, startZoned.getMonth)
    val end = YearMonth.of(endZoned.getYear, endZoned.getMonth)

    Iterator
      .iterate(start)(_.plusMonths(1))
      .takeWhile(!_.isAfter(end))
      .map(_.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      .toList
  }

  override def list(request: KVStore.ListRequest): Future[KVStore.ListResponse] = {
    logger.info(s"Performing list for data quality metrics dataset ${request.dataset}")

    val targetId = mapDatasetToTable(request.dataset)
    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      targetId.toString,
      metricsContext.copy(dataset = targetId.toString)
    )

    val query = Query
      .create(targetId)
      .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
      .filter(Filters.FILTERS.limit().cellsPerRow(10)) // Allow multiple qualifiers per row
      .limit(100)

    val startTs = System.currentTimeMillis()
    val rowsApiFuture = dataClient.readRowsCallable().all.futureCall(query)
    val rowsScalaFuture = BigTableKVStore.googleFutureToScalaFuture(rowsApiFuture)

    rowsScalaFuture
      .map { rows =>
        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("list.successes")

        val listValues = rows.asScala.flatMap { row =>
          row.getCells(ColumnFamilyString).asScala.map { cell =>
            KVStore.ListValue(row.getKey.toByteArray, cell.getValue.toByteArray)
          }
        }

        KVStore.ListResponse(request, Success(listValues), Map.empty)
      }
      .recover { case e: Exception =>
        logger.error("Error listing data quality metrics values", e)
        datasetMetricsContext.increment("list.bigtable_errors", Map("exception" -> e.getClass.getName))
        KVStore.ListResponse(request, Failure(e), Map.empty)
      }
  }

  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} data quality metrics requests")

    val resultFutures = requests.map { request =>
      val tableId = mapDatasetToTable(request.dataset)
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        tableId.toString,
        metricsContext.copy(dataset = tableId.toString)
      )

      val timestampInPutRequest = request.tsMillis
      val rowKey = buildDataQualityRowKey(request.keyBytes, timestampInPutRequest)

      // Use day-start timestamp for coarse granularity
      val timestampMillis = timestampInPutRequest.getOrElse(System.currentTimeMillis())
      val dayStartMillis = timestampMillis - (timestampMillis % 1.day.toMillis)
      val timestampMicros = dayStartMillis * 1000
      val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))

      // Store the value bytes as a blob under the column family
      val cellValue = ByteString.copyFrom(request.valueBytes)
      mutation.setCell(ColumnFamilyString, ColumnFamilyQualifier, timestampMicros, cellValue)

      val startTs = System.currentTimeMillis()
      val mutateApiFuture = dataClient.mutateRowAsync(mutation)
      val scalaFuture = BigTableKVStore.googleFutureToScalaFuture(mutateApiFuture)

      scalaFuture
        .map { _ =>
          datasetMetricsContext.distribution("multiPut.latency", System.currentTimeMillis() - startTs)
          datasetMetricsContext.increment("multiPut.successes")
          true
        }
        .recover { case e: Exception =>
          logger.error("Error putting data quality metrics data", e)
          datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
          false
        }
    }
    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    throw new UnsupportedOperationException("bulkPut is not supported for DataQualityMetricsKVStore")
  }

  /** Map dataset to table name for data quality metrics */
  def mapDatasetToTable(dataset: String): BTTableId = {
    val baseName = tableBaseName
    if (dataset.endsWith("_BATCH")) {
      BTTableId.of(s"${baseName}_BATCH")
    } else if (dataset.endsWith("_STREAMING")) {
      BTTableId.of(s"${baseName}_STREAMING")
    } else {
      BTTableId.of(s"${baseName}_BATCH")
    }
  }
}

private object BigTableMetricsKvStore {

  val ColumnFamilyString: String = "cf"
  val ColumnFamilyQualifierString: String = "value"
  val ColumnFamilyQualifier: ByteString = ByteString.copyFromUtf8(ColumnFamilyQualifierString)

  private def buildDataQualityRowKey(baseKeyBytes: Seq[Byte], maybeTs: Option[Long] = None): Array[Byte] = {
    (maybeTs match {
      case Some(ts) =>
        val yearMonth = formatYearMonth(ts)
        baseKeyBytes ++ s"#$yearMonth".getBytes(StandardCharsets.UTF_8)
      case _ => baseKeyBytes
    }).toArray
  }

  /** Format timestamp to year-month string */
  private def formatYearMonth(timestampMillis: Long): String = {
    val instant = Instant.ofEpochMilli(timestampMillis)
    DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC).format(instant)
  }
}
