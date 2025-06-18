package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.TaskTemplateIdMapConfig
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheTrait, TransformationIdMap}

class YamlConfigTransformationIdMapBaseConfig(
                                               systemCode: String,
                                               columnNames: List[String],
                                               domainName: String,
                                             ) {
  def getTaskTemplateIdMapConfig: TaskTemplateIdMapConfig = {
    TaskTemplateIdMapConfig(systemCode, columnNames, domainName)
  }

}
