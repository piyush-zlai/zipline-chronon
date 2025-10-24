package ai.chronon.flink.joinrunner

import ai.chronon.flink.DirectExecutionContext
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.Api
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Async function for performing join enrichment on streaming data.
  *
  * This function takes ProjectedEvent objects (from the left source after query application)
  * and enriches them with features from upstream joins, producing enriched ProjectedEvent objects
  * that contain both original fields and joined features.
  *
  * @param joinRequestName The name of the join to fetch (format: "joins/join_name")
  * @param api API implementation for fetcher access
  * @param enableDebug Whether to enable debug logging
  */
class JoinEnrichmentAsyncFunction(joinRequestName: String, groupByName: String, api: Api, enableDebug: Boolean)
    extends RichAsyncFunction[ProjectedEvent, ProjectedEvent] {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  @transient private var fetcher: Fetcher = _
  @transient private var successCounter: Counter = _
  @transient private var errorCounter: Counter = _
  @transient private var joinFetchLatencyHistogram: Histogram = _

  // The context used for the future callbacks
  implicit lazy val ec: ExecutionContext = JoinEnrichmentAsyncFunction.ExecutionContextInstance

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    logger.info("Initializing Fetcher for JoinEnrichmentAsyncFunction")
    fetcher = api.buildFetcher(debug = enableDebug)

    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("group_by", groupByName)
      .addGroup("join_enrichment", joinRequestName)
      
    successCounter = group.counter("join_fetch.successes")
    errorCounter = group.counter("join_fetch.errors")
    joinFetchLatencyHistogram = group.histogram(
      "join_fetch_latency",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new com.codahale.metrics.ExponentiallyDecayingReservoir())
      )
    )
    logger.info(s"JoinEnrichmentAsyncFunction initialized for join: $joinRequestName")
  }

  override def asyncInvoke(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    // Pass all left source field names to match Spark JoinSourceRunner approach
    val scalaKeyMap: Map[String, AnyRef] = event.fields.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.toMap
    // Create join request
    val request = Fetcher.Request(joinRequestName, scalaKeyMap)

    if (enableDebug) {
      logger.info(s"Join request: ${request.keys}, ts: ${request.atMillis}")
    }

    // Start latency measurement
    val startTime = System.currentTimeMillis()

    // Perform async join fetch
    val future = fetcher.fetchJoin(Seq(request))

    // Convert Scala Future to CompletableFuture for Flink
    val completableFuture = new CompletableFuture[ProjectedEvent]()

    future.onComplete {
      case Success(responses) =>
        // Record latency and increment success counter
        joinFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        successCounter.inc()
        
        if (responses.nonEmpty) {
          val response = responses.head
          val responseMap = response.values.getOrElse(Map.empty[String, Any])
          val enrichedFields = event.fields ++ responseMap

          if (enableDebug) {
            logger.info(
              s"Join response: request=${response.request.keys}, " +
                s"ts=${response.request.atMillis}, values=${response.values}")
          }

          val enrichedEvent = ProjectedEvent(
            enrichedFields,
            event.startProcessingTimeMillis
          )
          completableFuture.complete(enrichedEvent)
        } else {
          // No join response, pass through original event
          completableFuture.complete(event)
        }

      case Failure(ex) =>
        // Record latency and increment error counter
        joinFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        errorCounter.inc()
        
        logger.error("Error fetching join data", ex)
        completableFuture.completeExceptionally(ex)
    }

    // Complete the result future
    completableFuture.whenComplete { (result, ex) =>
      if (ex != null) {
        resultFuture.completeExceptionally(ex)
      } else {
        resultFuture.complete(java.util.Collections.singleton(result))
      }
    }
  }

  override def timeout(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    // Increment error counter for timeout
    errorCounter.inc()
    
    logger.warn(s"Join enrichment timeout for event: ${event.fields}")
    // On timeout, pass through the original event without enrichment
    resultFuture.complete(java.util.Collections.singleton(event))
  }
}

object JoinEnrichmentAsyncFunction {
  private val ExecutionContextInstance: ExecutionContext = new DirectExecutionContext
}
