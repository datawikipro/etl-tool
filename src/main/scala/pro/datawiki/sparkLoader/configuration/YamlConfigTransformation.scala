package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.{YamlConfigTransformationExtractJsonWithoutSchema, YamlConfigTransformationExtractSchema, YamlConfigTransformationIdmap, YamlConfigTransformationOutputSql, YamlConfigTransformationSparkSql}

class YamlConfigTransformation(objectName: String,
                               idmap: YamlConfigTransformationIdmap,
                               outputSql: YamlConfigTransformationOutputSql,
                               sparkSql: YamlConfigTransformationSparkSql,
                               extractSchema: YamlConfigTransformationExtractSchema,
                               extractJsonWithoutSchema: YamlConfigTransformationExtractJsonWithoutSchema
                              ) extends LogicClass {
  def getObjectName: String = objectName

  def getTransformation: YamlConfigTransformationTrait = {
    reset()
    setLogic(idmap)
    setLogic(outputSql)
    setLogic(sparkSql)
    setLogic(extractSchema)
    setLogic(extractJsonWithoutSchema)
    super.getLogic match
      case x: YamlConfigTransformationTrait => return x
      case _ => throw Exception()
  }

}
