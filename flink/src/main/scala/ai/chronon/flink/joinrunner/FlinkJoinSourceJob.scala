package ai.chronon.flink.joinrunner

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.flink.{AsyncKVStoreWriter, BaseFlinkJob, FlinkUtils, LateEventCounter, TiledAvroCodecFn}
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.types.{AvroCodecOutput, TimestampedTile, WriteResponse}
import ai.chronon.flink.window.{
  AlwaysFireOnElementTrigger,
  BufferedProcessingTimeTrigger,
  FlinkRowAggProcessFunction,
  FlinkRowAggregationFunction,
  KeySelectorBuilder
}
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}

import java.util.concurrent.TimeUnit
import scala.collection.Seq
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.OutputTag

/** Flink job implementation for chaining features using JoinSource GroupBys.
  *
  * This class is broadly based on the existing Spark JoinSourceRunner with corresponding Flink pieces.
  * For example, we use Async functions for the fetcher lookups, and we use catalyst util for the
  * Join source querySpark expr eval.
  *
  * @param eventSrc Provider of a Flink Datastream[ProjectedEvent] for the given topic and groupBy. The event
  *                  consists of a field Map as well as metadata columns such as processing start time (to track
  *                  metrics). The Map contains projected columns from the source data based on projections and filters
  *                  in the GroupBy.
  * @param inputSchema Schema of the left source (before enrichment)
  * @param sinkFn Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed The GroupBy we are working with
  * @param parallelism Parallelism to use for the Flink job
  * @param api API implementation for join fetching and KV store access
  * @param enableDebug If enabled will log additional debug info per processed event
  */
class FlinkJoinSourceJob(eventSrc: FlinkSource[ProjectedEvent],
                         inputSchema: Seq[(String, DataType)],
                         sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                         val groupByServingInfoParsed: GroupByServingInfoParsed,
                         parallelism: Int,
                         props: Map[String, String],
                         topicInfo: TopicInfo,
                         api: Api,
                         enableDebug: Boolean = false)
    extends BaseFlinkJob {

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink JoinSource streaming job. groupByName=${groupByName}")

  // The source of our Flink application is a topic
  val topic: String = topicInfo.name

  private val groupByConf = groupByServingInfoParsed.groupBy

  // Validate that this is a JoinSource configuration
  require(groupByConf.streamingSource.isDefined,
          s"No streaming source present in the groupBy: ${groupByConf.metaData.name}")
  require(groupByConf.streamingSource.get.isSetJoinSource,
          s"No JoinSource found in the groupBy: ${groupByConf.metaData.name}")

  val joinSource: JoinSource = groupByConf.streamingSource.get.getJoinSource
  val leftSource: Source = joinSource.getJoin.getLeft

  // Validate Events-based source
  require(leftSource.isSetEvents,
          s"Only Events-based sources are currently supported. Found: ${leftSource.getSetField}")

  val keyColumns: Array[String] = groupByConf.keyColumns.toScala.toArray
  val valueColumns: Array[String] = groupByConf.aggregationInputs
  val eventTimeColumn = Constants.TimeColumn

  // Configuration properties with defaults
  private val asyncTimeout: Long = FlinkUtils.getProperty("async_timeout_ms", props, topicInfo).getOrElse("5000").toLong
  private val asyncCapacity: Int = FlinkUtils.getProperty("async_capacity", props, topicInfo).getOrElse("100").toInt

  // Configuration properties with defaults
  private val kvStoreCapacity = FlinkUtils
    .getProperty("kv_concurrency", props, topicInfo)
    .map(_.toInt)
    .getOrElse(AsyncKVStoreWriter.kvStoreConcurrency)

  // We default to the AlwaysFireOnElementTrigger which will cause the window to "FIRE" on every element.
  // An alternative is the BufferedProcessingTimeTrigger (trigger=buffered in topic info
  // or properties) which will buffer writes and only "FIRE" every X milliseconds per GroupBy & key.
  private def getTrigger(): Trigger[ProjectedEvent, TimeWindow] = {
    FlinkUtils.getProperty("trigger", props, topicInfo).getOrElse("always_fire") match {
      case "always_fire" => new AlwaysFireOnElementTrigger()
      case "buffered"    => new BufferedProcessingTimeTrigger(100L)
      case t =>
        throw new IllegalArgumentException(s"Unsupported trigger type: $t. Supported: 'always_fire', 'buffered'")
    }
  }

  /** Build the tiled version of the Flink GroupBy job that chains features using a JoinSource.
    *  The operators are structured as follows:
    *  - Source: Read from Kafka topic into ProjectedEvent stream
    *  - Assign timestamps and watermarks based on event time column
    *  - Async Enrichment: Use JoinEnrichmentAsyncFunction to fetch join data asynchronously
    *  - Join Source Query: Apply join source query transformations using JoinSourceQueryFunction
    *  - Avro Conversion: Convert enriched events to AvroCodecOutput format for KV
    *  - Sink: Write to KV store using AsyncKVStoreWriter
    */
  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building Flink streaming job for groupBy: $groupByName that chains join: ${joinSource.getJoin.getMetaData.getName}" +
        s" using topic: $topic")

    // we expect parallelism on the source stream to be set by the source provider
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] = eventSrc
      .getDataStream(topic, groupByName)(env, parallelism)
      .uid(s"join-source-$groupByName")
      .name(s"Join Source for $groupByName")

    val watermarkedStream = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"join-source-watermarks-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Pass all left source field names to match Spark JoinSourceRunner approach
    val leftSourceFieldNames = inputSchema.map(_._1).toArray
    val enrichmentFunction = new JoinEnrichmentAsyncFunction(
      joinSource.join.metaData.getName,
      api,
      leftSourceFieldNames,
      enableDebug
    )

    val enrichedStream = AsyncDataStream
      .unorderedWait(
        watermarkedStream,
        enrichmentFunction,
        asyncTimeout,
        TimeUnit.MILLISECONDS,
        asyncCapacity
      )
      .uid(s"join-enrichment-$groupByName")
      .name(s"Async Join Enrichment for $groupByName")
      .setParallelism(watermarkedStream.getParallelism)

    // Apply join source query transformations only if there are transformations to apply
    val processedStream =
      if (joinSource.query != null && joinSource.query.selects != null && !joinSource.query.selects.isEmpty) {
        logger.info("Applying join source query transformations")
        val queryFunction = new JoinSourceQueryFunction(
          joinSource,
          inputSchema,
          api,
          enableDebug
        )

        enrichedStream
          .flatMap(queryFunction)
          .uid(s"join-source-query-$groupByName")
          .name(s"Join Source Query for $groupByName")
          .setParallelism(sourceSparkProjectedStream.getParallelism)
      } else {
        logger.info("No join source query transformations to apply - using enriched stream directly")
        enrichedStream
      }

    // Compute the output schema after JoinSourceQueryFunction transformations using Catalyst
    val postTransformationSchema = computePostTransformationSchemaWithCatalyst(joinSource, inputSchema)

    // Calculate tiling window size based on the GroupBy configuration
    val tilingWindowSizeInMillis: Long =
      ResolutionUtils.getSmallestTailHopMillis(groupByServingInfoParsed.groupBy)

    // Configure tumbling window for tiled aggregations
    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis))
      .asInstanceOf[WindowAssigner[ProjectedEvent, TimeWindow]]

    // Configure trigger (default to always fire on element)
    val trigger = getTrigger()

    // We use Flink "Side Outputs" to track any late events that aren't computed.
    val tilingLateEventsTag = new OutputTag[ProjectedEvent]("tiling-late-events") {}

    // Tiled aggregation: key by entity keys, window, and aggregate
    val tilingDS: SingleOutputStreamOperator[TimestampedTile] =
      processedStream
        .keyBy(KeySelectorBuilder.build(groupByServingInfoParsed.groupBy))
        .window(window)
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          // Aggregation function that maintains incremental IRs in state
          new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, postTransformationSchema, enableDebug),
          // Process function that marks tiles as closed for client-side caching
          new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, postTransformationSchema, enableDebug)
        )
        .uid(s"tiling-$groupByName")
        .name(s"Tiling for $groupByName")
        .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Track late events
    tilingDS
      .getSideOutput(tilingLateEventsTag)
      .flatMap(new LateEventCounter(groupByName))
      .uid(s"tiling-side-output-$groupByName")
      .name(s"Tiling Side Output Late Data for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Convert tiles to AvroCodecOutput format for KV store writing
    val avroConvertedStream = tilingDS
      .flatMap(TiledAvroCodecFn(groupByServingInfoParsed, tilingWindowSizeInMillis, enableDebug))
      .uid(s"avro-conversion-$groupByName")
      .name(s"Avro Conversion for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Write to KV store using existing AsyncKVStoreWriter
    AsyncKVStoreWriter.withUnorderedWaits(
      avroConvertedStream,
      sinkFn,
      groupByName,
      capacity = kvStoreCapacity
    )
  }

  /** Compute the schema that results after JoinSourceQueryFunction transformations.
    *  If there are no Query transforms defined in the JoinSource, we return the join schema
    *  (which includes the enrichment fields). Else, we get the output schema from CatalystUtil.
    */
  private def computePostTransformationSchemaWithCatalyst(
      joinSource: JoinSource,
      originalInputSchema: Seq[(String, DataType)]): Seq[(String, DataType)] = {
    if (joinSource.query == null || joinSource.query.selects == null || joinSource.query.selects.isEmpty) {
      // No transformations applied, return join schema (includes enrichment)
      try {
        val joinSchema = JoinSourceQueryFunction.buildJoinSchema(originalInputSchema, joinSource, api, enableDebug)
        joinSchema.fields.map { field =>
          (field.name, field.fieldType)
        }.toSeq
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to compute join schema, falling back to original schema", ex)
          originalInputSchema
      }
    } else {
      // Use shared method to determine the exact output schema
      try {
        val result = JoinSourceQueryFunction.buildCatalystUtil(joinSource, originalInputSchema, api, enableDebug)
        result.outputSchema
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to compute post-transformation schema using Catalyst, falling back to original schema",
                      ex)
          originalInputSchema
      }
    }
  }

}
