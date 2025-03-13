package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.YamlClass

class EltConfig(
                 connections: List[YamlConfigConnections] = List.apply(),
                 source: List[YamlConfigSource] = List.apply(),
                 idmap: String,
                 cache: String,
                 transformations: List[YamlConfigTransformation] = List.apply(),
                 target: List[YamlConfigTarget] = List.apply()
               ) {
  var segmentation: SegmentationEnum = SegmentationEnum.full

  def init(): Unit = {
    getSource.foreach(i => {
      i.getSegmentation match
        case SegmentationEnum.full =>
        case SegmentationEnum.partition =>
          segmentation = SegmentationEnum.partition
        case SegmentationEnum.random =>
          segmentation match
            case SegmentationEnum.full => {
              segmentation = SegmentationEnum.random
            }
            case SegmentationEnum.random => throw Exception()
            case _ => throw Exception()
        case SegmentationEnum.adHoc =>
        case _ => throw Exception()
    })
  }

  def getSegmentation: SegmentationEnum = {
    return segmentation
  }

  def getSegments: List[String] = {
    var listSegments: List[String] = List.apply()
    getSource.foreach(i => listSegments = listSegments ::: i.getSegments)
    return listSegments.distinct
  }

  def getSource: List[YamlConfigSource] = {
    if source == null then return List.apply()
    return source
  }

  def getTransformations: List[YamlConfigTransformation] = {
    if transformations == null then return List.apply()
    return transformations
  }

  def getIdmapSource: String = {
    return idmap
  }

  def getCacheSource: String = {
    return cache
  }

  def getConnections: List[YamlConfigConnections] = {
    if connections == null then return List.apply()
    return connections
  }

  def getTarget: List[YamlConfigTarget] = {
    return target
  }
}

object EltConfig extends YamlClass {
  def apply(inConfig: String): EltConfig = {
    val result = mapper.readValue(getLines(inConfig), classOf[EltConfig])
    result.init()
    return result
  }
}
