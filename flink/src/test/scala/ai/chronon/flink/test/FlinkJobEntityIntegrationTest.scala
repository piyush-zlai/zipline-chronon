package ai.chronon.flink.test

import ai.chronon.api.Constants.{ReversalColumn, TimeColumn}
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.GroupBy
import ai.chronon.flink.{FlinkGroupByStreamingJob, SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.collection.Seq

// Flink Job Integration Test for Entity-based GroupBys
class FlinkJobEntityIntegrationTest extends AnyFlatSpec with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  it should "flink job end to end" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // mutation timestamps are 1000ms (1s) after the created timestamps
    val elements = Seq(
      E2ETestMutationEvent("test1", 12, 1.5, 1699366993123L, 1699366993123L + 1000, isBefore = true),
      E2ETestMutationEvent("test2", 13, 1.6, 1699366993124L, 1699366993124L + 1000, isBefore = false),
      E2ETestMutationEvent("test3", 14, 1.7, 1699366993125L, 1699366993125L + 1000, isBefore = true)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val (job, groupByServingInfoParsed) = buildFlinkJob(groupBy, elements)
    job.runGroupByJob(env).addSink(new CollectSink)

    env.execute("FlinkJobEntityIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val writeEventCreatedDS = CollectSink.values.toScala

    writeEventCreatedDS.size shouldBe elements.size

    // check that the timestamps of the written out events match the input events - timestamp on the write requests matches
    // mutation time
    writeEventCreatedDS.map(_.tsMillis).toSet shouldBe elements.map(_.mutationTime).toSet
    // check that all the writes were successful
    writeEventCreatedDS.map(_.status) shouldBe Seq(true, true, true)

    // let's crack open the written value bytes and check that the values are correct
    val valueGenRecords =
      writeEventCreatedDS.map(in => groupByServingInfoParsed.mutationValueAvroCodec.decode(in.valueBytes))
    val mutationFieldsSet = valueGenRecords
      .map(r => (r.get(TimeColumn).asInstanceOf[Long], r.get(ReversalColumn).asInstanceOf[Boolean]))
      .toSet
    val expectedMutationFieldsSet = elements.map(e => (e.created, e.isBefore)).toSet

    mutationFieldsSet shouldBe expectedMutationFieldsSet
  }

  private def buildFlinkJob(groupBy: GroupBy,
                            elements: Seq[E2ETestMutationEvent]): (FlinkGroupByStreamingJob, GroupByServingInfoParsed) = {
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val sparkExpressionEvalFn = new SparkExpressionEvalFn(Encoders.product[E2ETestMutationEvent], query, groupBy.metaData.name, groupBy.dataModel)
    val source = new WatermarkedE2ETestMutationEventSource(elements, sparkExpressionEvalFn)

    // Prepare the Flink Job
    val encoder = Encoders.product[E2ETestMutationEvent]
    val outputSchema = new SparkExpressionEval(encoder, query, groupBy.getMetaData.getName, groupBy.dataModel).getOutputSchema
    val outputSchemaDataTypes = outputSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val mockApi = mock[Api](withSettings().serializable())
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), mockApi, groupBy.metaData.name)
    val topicInfo = TopicInfo.parse("kafka://test-topic")
    (new FlinkGroupByStreamingJob(source,
                  outputSchemaDataTypes,
                  writerFn,
                  groupByServingInfoParsed,
                  2,
                  props = Map.empty,
                  topicInfo = topicInfo),
     groupByServingInfoParsed)
  }
}
