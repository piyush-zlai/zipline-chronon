package ai.chronon.flink.validation

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.test.{CollectSink, FlinkTestUtils}
import ai.chronon.flink.SparkExpressionEval
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util
import java.util.Collections

class RowEventSource(mockEvents: Seq[Row]) extends FlinkSource[Row] {

  implicit val parallelism: Int = 1

  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): SingleOutputStreamOperator[Row] = {
    env.fromCollection(mockEvents.toJava)
  }
}

class StatsCollectSink extends SinkFunction[ValidationStats] {
  override def invoke(value: ValidationStats, context: SinkFunction.Context): Unit = {
    StatsCollectSink.values.add(value)
  }
}

object StatsCollectSink {
  // must be static
  val values: util.List[ValidationStats] = Collections.synchronizedList(new util.ArrayList())
}

class ValidationFlinkJobIntegrationTest extends AnyFlatSpec with BeforeAndAfter {

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

  it should "run catalyst and spark df side by side" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val elements = Seq(
      Row("test1", 12, 1.5, 1699366993123L),
      Row("test2", 13, 1.6, 1699366993124L),
      Row("test3", 14, 1.7, 1699366993125L)
    )

    val source = new RowEventSource(elements)
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val fields = Seq(StructField("id", StringType),
                     StructField("int_val", IntegerType),
                     StructField("double_val", DoubleType),
                     StructField("created", LongType))
    val encoder = Encoders.row(StructType(fields))

    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val outputSchema = new SparkExpressionEval(encoder, query, groupBy.getMetaData.getName, groupBy.dataModel).getOutputSchema

    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)
    val job = new ValidationFlinkJob(source, groupByServingInfoParsed, encoder, 2, elements.size)
    job.runValidationJob(env).addSink(new StatsCollectSink)

    env.execute("FlinkValidationJobIntegrationTest")

    // capture the datastream of the 'created' timestamps of all the written out events
    val validationStatsDs = StatsCollectSink.values.toScala
    validationStatsDs.size shouldBe 1

    val validationStats = validationStatsDs.head
    validationStats.totalRecords shouldBe elements.size
    validationStats.totalMatches shouldBe elements.size
    validationStats.catalystRowCount shouldBe elements.size
    validationStats.sparkDfRowCount shouldBe elements.size
    validationStats.totalMismatches shouldBe 0
  }
}
