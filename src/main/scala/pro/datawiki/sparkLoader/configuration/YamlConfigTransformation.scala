package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.YamlConfigTransformationIdmap
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.YamlConfigTransformationOutputSql
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.YamlConfigTransformationSparkSql

class YamlConfigTransformation(objectName: String,
                               idmap: YamlConfigTransformationIdmap,
                               outputSql: YamlConfigTransformationOutputSql,
                               sparkSql: YamlConfigTransformationSparkSql
                              ) {
  def getObjectName: String = objectName

  def getTransformation: YamlConfigTransformationTrait = {
    var checkCounter: Int = 0
    var task: YamlConfigTransformationTrait = null

    if idmap != null then {
      task = idmap
      checkCounter += 1
    }
    if outputSql != null then {
      task = outputSql
      checkCounter += 1
    }
    if sparkSql != null then {
      task = sparkSql
      checkCounter += 1
    }

    checkCounter match
      case 0 => throw Exception()
      case 1 => return task
      case _ => throw Exception()

  }

}
