package ai.chronon.flink

import ai.chronon.online.TopicInfo

import scala.concurrent.ExecutionContext

object FlinkUtils {

  def getProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): Option[String] = {
    props
      .get(key)
      .filter(_.nonEmpty)
      .orElse {
        topicInfo.params.get(key)
      }
  }
}

/** This was moved to flink-rpc-akka in Flink 1.16 and made private, so we reproduce the direct execution context here
  */
private class DirectExecutionContext extends ExecutionContext {
  override def execute(runnable: Runnable): Unit =
    runnable.run()

  override def reportFailure(cause: Throwable): Unit =
    throw new IllegalStateException("Error in direct execution context.", cause)

  override def prepare: ExecutionContext = this
}
