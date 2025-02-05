package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{EltConfig, SegmentationEnum, YamlConfigConnections, YamlConfigSource, YamlConfigTarget, YamlConfigTransformation}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, WriteMode}
import pro.datawiki.sparkLoader.source.Source
import pro.datawiki.sparkLoader.target.Target
import pro.datawiki.sparkLoader.transformation.{Transformation, TransformationCache, TransformationIdMap}

object Execute {
  def initConnections(connections: List[YamlConfigConnections]):Unit = {
    connections.foreach(i => Connection.setConnection(i.sourceName, Connection.initConnection(i.connection, i.configLocation)))
  }

  def setIdmap(in: String): Unit = {
    TransformationIdMap.setIdmap(in)
  }

  def setCache(in: String): Unit = {
    TransformationCache.setBaseCache(in)
  }

  def setTarget(in: String): Unit = {
    Target.setTarget(in)
  }

  def run(etlConfig:EltConfig): Unit = {
    etlConfig.getSegmentation match
      case SegmentationEnum.full =>
        Source.run(etlConfig.getSource, null)
        Transformation.run(etlConfig.getTransformations)
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

  def writeTarget(target: YamlConfigTarget): Unit = {
    target.uniqueKey.isEmpty match
      case true => Target.writeTarget(target.targetFile,target.getMode)
      case false => Target.writeTarget(target.targetFile, target.uniqueKey,target.getColumns,target.getMode)

  }

}
