package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.SegmentationEnum.{full, random}
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.Connection
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.{YamlConfigSourceAdHoc, YamlConfigSourceDBSQL, YamlConfigSourceDBTable, YamlConfigSourceFileSystem, YamlConfigSourceKafkaTopic, YamlConfigSourceWeb}

class YamlConfigSource(sourceName: String,
                       objectName: String,
                       segmentation: String,
                       sourceDb: YamlConfigSourceDBTable,
                       sourceSQL: YamlConfigSourceDBSQL,
                       sourceFileSystem: YamlConfigSourceFileSystem,
                       sourceKafkaTopic: YamlConfigSourceKafkaTopic,
                       sourceWeb: YamlConfigSourceWeb,
                       adHoc: YamlConfigSourceAdHoc
                      ) extends LogicClass{

  def getSourceName: String = sourceName

  def getObjectName: String = objectName

  def getSegmentation: SegmentationEnum = {
    if isAdHoc then {
      return SegmentationEnum.adHoc
    }
    if segmentation == null then {
      return SegmentationEnum.full
    }
    segmentation match
      case "random" => return SegmentationEnum.random
      case "full" => return SegmentationEnum.full
      case "partition" => return SegmentationEnum.partition
      case _ => throw Exception()
  }

  def getSegments: List[String] = {
    return getSource.getSegments(Connection.getConnection(sourceName))
  }

  def getSource: YamlConfigSourceTrait = {
    reset()
    setLogic(sourceDb)
    setLogic(sourceSQL)
    setLogic(sourceFileSystem)
    setLogic(sourceKafkaTopic)
    setLogic(sourceWeb)

    super.getLogic match
      case x:YamlConfigSourceTrait => return x
      case _ => throw Exception()
  }

  def isAdHoc: Boolean={
    adHoc match
      case null => false
      case _ => true
  }

  def getAdhocRow: List[Row] = {
    return adHoc.getAdhocRow
  }

}