package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplateIdMapConfig
import pro.datawiki.sparkLoader.transformation.TransformationCache

case class YamlConfigTransformationIdMapBaseConfig(
                                                    systemCode: String,
                                                    columnNames: List[String],
                                                    domainName: String,
                                                  ) {
  def getTaskTemplateIdMapConfig: TaskTemplateIdMapConfig = {
    TaskTemplateIdMapConfig(systemCode, columnNames, domainName)
  }

}
