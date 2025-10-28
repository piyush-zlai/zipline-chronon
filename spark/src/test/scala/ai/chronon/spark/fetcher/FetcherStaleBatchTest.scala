package ai.chronon.spark.fetcher

import ai.chronon.api.TsUtils
import ai.chronon.api.Extensions._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.{Accuracy, Builders, Operation, TimeUnit, Window}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.{FlagStore, FlagStoreConstants}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import java.{lang, util}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

class FetcherStaleBatchTest extends SparkTestBase {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "demonstrate bug in Fetcher.fetchJoin for old expired feature data" in {
    val namespace = "stale_batch_temporal_test"

    // Generate the standard event data - we generate events in the 2021-04-05 to 2021-04-10 range
    // We create a Join which includes a GroupBy with temporal accuracy and two aggregations (SUM over 7d, AVG over 30d)
    val joinConf = FetcherStaleBatchTest.generateEventOnlyData(namespace, spark)

    // Load up data and set up various components like MockApi, MetadataStore for
    // enabling fetch calls
    val mockApi = FetcherStaleBatchTest.setupTemporalFetch(
      joinConf = joinConf,
      endDs = "2021-04-10", // Setup with batch data ending at 2021-04-10 (inclusive)
      namespace = namespace,
      dropDsOnWrite = true,
      enableTiling = true
    )(spark)

    // Now make a fetch call with a key and timestamp from much later (2022-04-06)
    val keyMap = Map("listing_id" -> java.lang.Long.valueOf(1L)) // 1L to trigger test failure, 4L to see correct response
    val queryTimestamp = TsUtils.datetimeToTs("2022-04-06 12:00:00") // Query ~1 year after data
    val joinName = joinConf.metaData.name

    val response = FetcherStaleBatchTest.fetchJoinWithMockApi(
      mockApi = mockApi,
      keyMap = keyMap,
      ts = queryTimestamp,
      joinName = joinName
    )

    response.values match {
      case Success(values) =>
        val nonNullValues = values.filter { case (_, v) => v != null }
        assert(nonNullValues.isEmpty,
          s"Expected all null values due to expired data, but got: $nonNullValues")

      case Failure(exception) =>
        throw exception
    }
  }
}

object FetcherStaleBatchTest {
  
  def generateEventOnlyData(namespace: String,
                            spark: SparkSession,
                            groupByCustomJson: Option[String] = None): api.Join = {
    SparkTestBase.createDatabase(spark, namespace)

    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    val listingEventData = Seq(
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10"),
      Row(2L, toTs("2021-04-10 03:10:00"), "2021-04-10")
    )
    val ratingEventData = Seq(
      // 1L listing id event data
      Row(1L, toTs("2021-04-05 00:30:00"), 2, "2021-04-05"),
      Row(1L, toTs("2021-04-06 05:35:00"), 4, "2021-04-06"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      // 2L listing id event data
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"),
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"),
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"),
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"),
      Row(2L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(2L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
    )
    // Schemas
    // {..., event (generic event column), ...}
    val listingsSchema = api.StructType("listing_events_fetcher",
                                    Array(
                                      api.StructField("listing_id", api.LongType),
                                      api.StructField("ts", api.LongType),
                                      api.StructField("ds", api.StringType)
                                    ))

    val ratingsSchema = api.StructType(
      "listing_ratings_fetcher",
      Array(api.StructField("listing_id", api.LongType),
            api.StructField("ts", api.LongType),
            api.StructField("rating", api.IntType),
            api.StructField("ds", api.StringType))
    )

    val sourceData: Map[api.StructType, Seq[Row]] = Map(
      listingsSchema -> listingEventData,
      ratingsSchema -> ratingEventData
    )

    sourceData.foreach { case (schema, rows) =>
      val tableName = s"$namespace.${schema.name}"

      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      spark
        .createDataFrame(rows.asJava, SparkConversions.fromChrononSchema(schema))
        .save(tableName)
    }
    println("saved all data hand written for fetcher test")

    val startPartition = "2021-04-05"

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${listingsSchema.name}"
      )

    val rightSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts", "rating"),
          startPartition = startPartition
        ),
        table = s"$namespace.${ratingsSchema.name}",
        topic = "fake_topic2"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        ),
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(
        name = "unit_test/fetcher_tiled_gb",
        namespace = namespace,
        team = "chronon",
        customJson = groupByCustomJson.orNull
      )
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def setupTemporalFetch(joinConf: api.Join,
                         endDs: String,
                         namespace: String,
                         dropDsOnWrite: Boolean,
                         enableTiling: Boolean = false)(implicit spark: SparkSession): MockApi = {
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()

    val tilingEnabledFlagStore = new FlagStore {
      override def isSet(flagName: String, attributes: util.Map[String, String]): lang.Boolean = {
        if (flagName == FlagStoreConstants.TILING_ENABLED) {
          enableTiling
        } else {
          false
        }
      }
    }

    val mockApi = new MockApi(kvStoreFunc, namespace)
    mockApi.setFlagStore(tilingEnabledFlagStore)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)

    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        endDs,
                        jp.groupBy,
                        dropDsOnWrite = dropDsOnWrite,
                        tilingEnabled = enableTiling))

    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)
    
    mockApi
  }

  def fetchJoinWithMockApi(mockApi: MockApi, 
                          keyMap: Map[String, AnyRef], 
                          ts: Long, 
                          joinName: String): Response = {
    val fetcher = mockApi.buildFetcher()
    val request = Request(joinName, keyMap, Some(ts))
    val futureResponse = fetcher.fetchJoin(Array(request))
    val responses = Await.result(futureResponse, Duration(10, SECONDS))
    responses.head
  }
}