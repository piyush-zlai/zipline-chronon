package ai.chronon.flink.joinrunner

import ai.chronon.api.{Constants, DataType, JoinSource, StructField, StructType}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.{Api, CatalystUtil, JoinCodec}
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

/** Flink function for applying join source query transformations to enriched events.
  *
  * This function takes ProjectedEvent objects (after join enrichment) and applies
  * the joinSource.query SQL expressions to produce the final processed events
  * for aggregation and storage.
  *
  * @param joinSource The JoinSource configuration containing the query to apply
  * @param inputSchema Schema of the left source (before enrichment)
  * @param api API implementation for join codec access
  * @param enableDebug Whether to enable debug logging
  */
class JoinSourceQueryFunction(joinSource: JoinSource,
                              inputSchema: Seq[(String, DataType)],
                              api: Api,
                              enableDebug: Boolean)
    extends RichFlatMapFunction[ProjectedEvent, ProjectedEvent] {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  @transient private var catalystUtil: CatalystUtil = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    logger.info("Initializing CatalystUtil for join source query evaluation")

    val result = JoinSourceQueryFunction.buildCatalystUtil(joinSource, inputSchema, api, enableDebug)
    catalystUtil = result.catalystUtil

    logger.info(s"Initialized CatalystUtil with join schema")
  }

  override def flatMap(enrichedEvent: ProjectedEvent, out: Collector[ProjectedEvent]): Unit = {
    try {
      if (catalystUtil != null) {
        // Apply join source query to the enriched event fields
        val scalaFields = enrichedEvent.fields
        val queryResults = catalystUtil.performSql(scalaFields)

        if (enableDebug) {
          logger.info(s"Join source query input: ${scalaFields}")
          logger.info(s"Join source query results: $queryResults")
        }

        // Output each result as a ProjectedEvent
        queryResults.foreach { resultFields =>
          val resultEvent = ProjectedEvent(resultFields, enrichedEvent.startProcessingTimeMillis)
          out.collect(resultEvent)
        }
      } else {
        // No query to apply, pass through the enriched event
        out.collect(enrichedEvent)
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error applying join source query to event: ${enrichedEvent.fields}", ex)
        // On error, pass through the original enriched event
        out.collect(enrichedEvent)
    }
  }
}

case class JoinSourceQueryResult(
    catalystUtil: CatalystUtil,
    joinSchema: StructType,
    outputSchema: Seq[(String, DataType)]
)

object JoinSourceQueryFunction {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def buildCatalystUtil(
      joinSource: JoinSource,
      inputSchema: Seq[(String, DataType)],
      api: Api,
      enableDebug: Boolean
  ): JoinSourceQueryResult = {

    // Build join schema (leftSourceSchema + joinCodec.valueSchema)
    val joinSchema = buildJoinSchema(inputSchema, joinSource, api, enableDebug)

    // Handle time column mapping like SparkExpressionEval.buildQueryTransformsAndFilters
    val timeColumn = Option(joinSource.query.timeColumn).getOrElse(Constants.TimeColumn)
    val rawSelects = joinSource.query.selects.asScala.toMap

    val timeColumnMapping = Map(Constants.TimeColumn -> timeColumn) // Add ts -> timeColumn mapping
    val selectsWithTimeColumn = (rawSelects ++ timeColumnMapping).toSeq
    val wheres = Option(joinSource.query.wheres).map(_.asScala).getOrElse(Seq.empty)

    // Create CatalystUtil instance
    val catalystUtil = new CatalystUtil(joinSchema, selectsWithTimeColumn, wheres)

    // Get the output schema from Catalyst and convert to Chronon format
    val outputSparkSchema = catalystUtil.getOutputSparkSchema
    val outputSchema = outputSparkSchema.fields.map { field =>
      val chrononType = SparkConversions.toChrononType(field.name, field.dataType)
      (field.name, chrononType)
    }.toSeq

    logger.info(s"Built CatalystUtil for join source query:")
    logger.info(s"Query selects: $selectsWithTimeColumn, wheres: $wheres")
    logger.info(s"Time column mapping: ${Constants.TimeColumn} -> $timeColumn")
    logger.info(s"Output schema: ${outputSchema.map { case (name, dataType) => s"$name: $dataType" }.mkString(", ")}")

    JoinSourceQueryResult(catalystUtil, joinSchema, outputSchema)
  }

  /** Build the join schema following JoinSourceRunner.buildSchemas approach:
    * joinSchema = leftSourceSchema ++ joinCodec.valueSchema
    */
  def buildJoinSchema(
      inputSchema: Seq[(String, DataType)],
      joinSource: JoinSource,
      api: Api,
      enableDebug: Boolean
  ): StructType = {
    // leftSourceSchema: Convert inputSchema to Chronon StructType
    val leftSourceFields = inputSchema.map { case (name, dataType) =>
      StructField(name, dataType)
    }.toArray
    val leftSourceSchema = StructType("left_source", leftSourceFields)

    // joinCodec.valueSchema: Get schema of enriched fields from upstream join
    val joinCodec: JoinCodec = api
      .buildFetcher(debug = enableDebug)
      .metadataStore
      .buildJoinCodec(joinSource.getJoin, refreshOnFail = false)

    // joinSchema = leftSourceSchema ++ joinCodec.valueSchema
    val joinFields = leftSourceSchema.fields ++ joinCodec.valueSchema.fields
    val joinSchema = StructType("join_enriched", joinFields)

    logger.info(s"""
         |Schema building for join source query:
         |leftSourceSchema (${leftSourceFields.length} fields):
         |  ${leftSourceFields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |joinCodec.valueSchema (${joinCodec.valueSchema.fields.length} fields):
         |  ${joinCodec.valueSchema.fields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |joinSchema (${joinFields.length} fields):
         |  ${joinFields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |""".stripMargin)

    joinSchema
  }
}
