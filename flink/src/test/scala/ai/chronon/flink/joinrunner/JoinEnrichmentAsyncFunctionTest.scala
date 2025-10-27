package ai.chronon.flink.joinrunner

import ai.chronon.api.Constants
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.Api
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.metrics.{Counter, Histogram, MetricGroup}
import org.apache.flink.metrics.groups.OperatorMetricGroup
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.Promise

class JoinEnrichmentAsyncFunctionTest extends AnyFlatSpec with Matchers with MockitoSugar {

  val joinRequestName = "joins/test_team/test_join"
  val enableDebug = false

  private def setupFunctionWithMockedMetrics(function: JoinEnrichmentAsyncFunction): Unit = {
    // Mock the runtime context and metrics
    val mockRuntimeContext = mock[RuntimeContext]
    val mockOperatorMetricGroup = mock[OperatorMetricGroup]
    val mockSubGroup = mock[MetricGroup]
    val mockCounter = mock[Counter]
    val mockHistogram = mock[Histogram]
    
    // Mock the metric group chain
    when(mockRuntimeContext.getMetricGroup).thenReturn(mockOperatorMetricGroup)
    when(mockOperatorMetricGroup.addGroup("chronon")).thenReturn(mockSubGroup)
    when(mockSubGroup.addGroup(anyString(), anyString())).thenReturn(mockSubGroup)
    when(mockSubGroup.counter(anyString())).thenReturn(mockCounter)
    when(mockSubGroup.histogram(anyString(), any())).thenReturn(mockHistogram)
    
    function.setRuntimeContext(mockRuntimeContext)
  }

  "JoinEnrichmentAsyncFunction" should "enrich events with join response" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)
    
    // Create successful join response
    val joinResponse = Fetcher.Response(
      Fetcher.Request(joinRequestName, Map("user_id" -> "123"), Some(2000L)),
      scala.util.Success(Map("user_category" -> "premium", "user_score" -> 85.5).asInstanceOf[Map[String, AnyRef]])
    )
    val joinFuture = Promise[Seq[Fetcher.Response]]()
    joinFuture.success(Seq(joinResponse))
    when(mockFetcher.fetchJoin(any(), any())).thenReturn(joinFuture.future)
    
    val function = new JoinEnrichmentAsyncFunction(joinRequestName, "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())
    
    // Create test event
    val eventFields = Map("user_id" -> "123", "price" -> 99.99, Constants.TimeColumn -> 1000L)
    val event = ProjectedEvent(eventFields, 500L)
    
    // Mock result future
    val latch = new CountDownLatch(1)
    var result: ProjectedEvent = null
    val resultFuture = new ResultFuture[ProjectedEvent] {
      override def complete(results: java.util.Collection[ProjectedEvent]): Unit = {
        result = results.iterator().next()
        latch.countDown()
      }
      override def completeExceptionally(throwable: Throwable): Unit = {
        throwable.printStackTrace()
        latch.countDown()
      }
    }
    
    // Execute async invocation
    function.asyncInvoke(event, resultFuture)
    
    // Wait for completion
    latch.await(5, TimeUnit.SECONDS) should be(true)
    
    // Verify results
    result should not be null
    result.startProcessingTimeMillis should be(500L)
    
    val fields = result.fields
    fields("user_id") should be("123")
    fields("price") should be(99.99)
    fields("user_category") should be("premium")
    fields("user_score") should be(85.5)
    
    // Verify the join request was made
    verify(mockFetcher).fetchJoin(any(), any())
  }

  it should "handle join timeout gracefully" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)
    
    val function = new JoinEnrichmentAsyncFunction(joinRequestName, "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())
    
    // Create test event
    val eventFields = Map("user_id" -> "123", "price" -> 99.99, Constants.TimeColumn -> 1000L)
    val event = ProjectedEvent(eventFields, 500L)
    
    // Mock result future
    val latch = new CountDownLatch(1)
    var result: ProjectedEvent = null
    val resultFuture = new ResultFuture[ProjectedEvent] {
      override def complete(results: java.util.Collection[ProjectedEvent]): Unit = {
        result = results.iterator().next()
        latch.countDown()
      }
      override def completeExceptionally(throwable: Throwable): Unit = {
        latch.countDown()
      }
    }
    
    // Execute timeout
    function.timeout(event, resultFuture)
    
    // Wait for completion
    latch.await(5, TimeUnit.SECONDS) should be(true)
    
    // Verify timeout result (original event passed through)
    result should not be null
    result.startProcessingTimeMillis should be(500L)
    
    val fields = result.fields
    fields("user_id") should be("123")
    fields("price") should be(99.99)
    fields should not contain key("user_category") // No enrichment
  }

  it should "handle empty join response" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)
    
    // Create empty join response
    val joinFuture = Promise[Seq[Fetcher.Response]]()
    joinFuture.success(Seq.empty)
    when(mockFetcher.fetchJoin(any(), any())).thenReturn(joinFuture.future)
    
    val function = new JoinEnrichmentAsyncFunction(joinRequestName, "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())
    
    // Create test event
    val eventFields = Map("user_id" -> "123", "price" -> 99.99, Constants.TimeColumn -> 1000L)
    val event = ProjectedEvent(eventFields, 500L)
    
    // Mock result future
    val latch = new CountDownLatch(1)
    var result: ProjectedEvent = null
    val resultFuture = new ResultFuture[ProjectedEvent] {
      override def complete(results: java.util.Collection[ProjectedEvent]): Unit = {
        result = results.iterator().next()
        latch.countDown()
      }
      override def completeExceptionally(throwable: Throwable): Unit = {
        latch.countDown()
      }
    }
    
    // Execute async invocation
    function.asyncInvoke(event, resultFuture)
    
    // Wait for completion
    latch.await(5, TimeUnit.SECONDS) should be(true)
    
    // Verify results (original event passed through)
    result should not be null
    val fields = result.fields
    fields("user_id") should be("123")
    fields("price") should be(99.99)
    fields should not contain key("user_category") // No enrichment
  }
}