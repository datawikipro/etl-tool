package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, SegmentationEnum, YamlConfigConnections, YamlConfigTarget}
import pro.datawiki.sparkLoader.connection.Connection
import pro.datawiki.sparkLoader.transformation.{Transformation, TransformationIdMap}

object Execute {
  def initConnections(connections: List[YamlConfigConnections]): Unit = {
    connections.foreach(i => Connection.setConnection(i.sourceName, Connection.initConnection(i.connection, i.configLocation)))
  }

  def setIdmap(in: String): Unit = {
    TransformationIdMap.setIdmap(in)
  }

  def run(etlConfig: EltConfig): Unit = {
    etlConfig.getSegmentation match
      case SegmentationEnum.full => {
        etlConfig.getSource.foreach(i => i.run(segmentName = null))
        Transformation.run(etlConfig.getTransformations)
      }
      case SegmentationEnum.random =>
        throw Exception()
      //        var i1 = 0
      //        etlConfig.getSegments.foreach(i => {
      //          if i1 <= 5 then {
      //            Source.run(etlConfig.getSource, i)
      //            Transformation.run(etlConfig.getTransformations)
      //            
      //            Target.setTargetCache(i1)
      //            i1 += 1
      //          }
      //        })
      //        Target.getTargetCache.createOrReplaceTempView("target")
      case _ => throw Exception()
  }
}
