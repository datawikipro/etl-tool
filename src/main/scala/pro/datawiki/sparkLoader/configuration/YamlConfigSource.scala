package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.{YamlConfigSourceDBTable, YamlConfigSourceKafkaTopic}

class YamlConfigSource(sourceName: String,
                       objectName: String,
                       sourceDb: YamlConfigSourceDBTable,
                       sourceKafkaTopic: YamlConfigSourceKafkaTopic
                      ) {
  def getSourceName: String = sourceName

  def getObjectName: String = objectName

  def getSource: YamlConfigSourceTrait = {
    var checkCounter: Int = 0
    var task: YamlConfigSourceTrait = null

    if sourceDb != null then {
      task = sourceDb
      checkCounter += 1
    }
    if sourceKafkaTopic != null then {
      task = sourceKafkaTopic
      checkCounter += 1
    }
    
    checkCounter match
      case 0 => throw Exception()
      case 1 => return task
      case _ => throw Exception()
    
  }


}