package ai.chronon.flink.test

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.flink.{SparkExpressionEval, SparkExpressionEvalFn}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.Encoders
import org.scalatest.flatspec.AnyFlatSpec

class SparkExpressionEvalFnTest extends AnyFlatSpec {

  it should "perform basic spark expr eval checks for event based groupbys" in {

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      query,
      groupBy.metaData.name
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    // let's check the size
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
    // let's check the id field
    assert(result.map(_.apply("id")).toSet == Set("test1", "test2", "test3"))
  }

  it should "allow groupbys to have null filters" in {

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"), filters = null)
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)

    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      query,
      groupBy.metaData.name
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
  }

  it should "perform basic spark expr eval checks for entity based groupbys" in {

    val elements = Seq(
      E2ETestMutationEvent("test1", 12, 1.5, 1699366993123L, 1699366993123L, isBefore = true),
      E2ETestMutationEvent("test2", 13, 1.6, 1699366993124L, 1699366993124L, isBefore = false),
      E2ETestMutationEvent("test3", 14, 1.7, 1699366993125L, 1699366993125L, isBefore = true)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val encoder = Encoders.product[E2ETestMutationEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestMutationEvent](
      encoder,
      query,
      groupBy.metaData.name,
      groupBy.dataModel
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestMutationEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    // let's check the size
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
    // let's check the id field
    assert(result.map(_.apply("id")).toSet == Set("test1", "test2", "test3"))
    // let's check the isBefore field
    assert(result.map(_.apply("is_before")).toSet == Set(true, false))
  }
}
