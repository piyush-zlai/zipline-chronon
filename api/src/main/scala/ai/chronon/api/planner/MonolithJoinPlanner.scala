package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Join, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.planner
import ai.chronon.planner.Node

import scala.collection.JavaConverters._

case class MonolithJoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private def semanticMonolithJoin(join: Join): Join = {
    val semanticJoin = join.deepCopy()
    semanticJoin.unsetMetaData()
    Option(semanticJoin.joinParts).map(_.asScala).foreach { parts =>
      parts.foreach(joinPart => joinPart.groupBy.unsetMetaData())
    }
    Option(semanticJoin.bootstrapParts).map(_.asScala).foreach { bootstrapParts =>
      bootstrapParts.foreach(bootstrapPart => bootstrapPart.unsetMetaData())
    }
    Option(semanticJoin.labelParts).map(_.labels).map(_.asScala).foreach { labelPart =>
      labelPart.foreach { joinPart => joinPart.groupBy.unsetMetaData() }
    }
    semanticJoin.unsetOnlineExternalParts()
    semanticJoin

  }

  def backfillNode: Node = {
    val tableDeps = TableDependencies.fromJoin(join)

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "backfill",
                          join.metaData.name + "__backfill",
                          tableDeps,
                          outputTableOverride = Some(join.metaData.outputTable))
    val node = new planner.MonolithJoinNode().setJoin(join)
    toNode(metaData, _.setMonolithJoin(node), semanticMonolithJoin(join))
  }

  def metadataUploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies for all GroupBy parts (both direct GroupBy deps and upstream join deps)
    val allDeps = Option(join.joinParts).map(_.asScala).getOrElse(Seq.empty).flatMap { joinPart =>
      val groupBy = joinPart.groupBy
      val hasStreamingSource = groupBy.streamingSource.isDefined

      // Add dependency on the GroupBy node (either uploadToKV or streaming)
      val groupByTableName = if (hasStreamingSource) {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.Streaming}"
      } else {
        groupBy.metaData.outputTable + s"__${GroupByPlanner.UploadToKV}"
      }

      val groupByDep = new TableDependency()
        .setTableInfo(
          new TableInfo()
            .setTable(groupByTableName)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      // Add dependencies on upstream join metadata uploads if GroupBy has JoinSource
      val upstreamJoinDeps = if (hasStreamingSource) {
        // Skip this for streaming GroupBys since the streaming node will handle this dependency
        Seq.empty
      } else {
        TableDependencies.fromJoinSources(groupBy.sources)
      }

      // Return both the GroupBy dependency and any upstream join dependencies
      Seq(groupByDep) ++ upstreamJoinDeps
    }

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "metadata_upload",
                          join.metaData.name + "__metadata_upload",
                          allDeps,
                          Some(stepDays))
    val node = new planner.JoinMetadataUpload().setJoin(join)
    toNode(metaData, _.setJoinMetadataUpload(node), semanticMonolithJoin(join))
  }

  override def buildPlan: planner.ConfPlan = {
    val confPlan = new planner.ConfPlan()

    val backfill = backfillNode
    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)
      .map((es) =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es)))

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfill.metaData.name,
      planner.Mode.DEPLOY -> metadataUploadNode.metaData.name
    )

    confPlan
      .setNodes((List(backfill, metadataUploadNode) ++ sensorNodes).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
