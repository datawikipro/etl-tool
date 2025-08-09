package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateTransformation
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.{CoreTaskTemplateTransformationExtractAndValidateDataFrame, CoreTaskTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.*

class YamlDataTemplateTransformation(
                                      objectName: String,
                                      cache: String,
                                      idMap: YamlDataTemplateTransformationIdMap,
                                      sparkSql: YamlDataTemplateTransformationSparkSql,
                                      extractSchema: YamlDataTemplateTransformationExtractSchema,
                                      extractAndValidateDataFrame: YamlDataTemplateTransformationExtractAndValidateDataFrame,
                                      adHoc: YamlDataTemplateTransformationAdHoc,

                                    ) {
  def getCoreTransformation: CoreTaskTemplateTransformation = {
    CoreTaskTemplateTransformation(
      objectName = objectName,
      cache = cache,
      idMap = if idMap != null then {
        throw Exception()
      } else null,
      sparkSql = sparkSql match {
        case null => null
        case other => CoreTaskTemplateTransformationSparkSql(
          sql = other.sql,
          isLazyTransform = other.isLazyTransform,
          lazyTable = other.lazyTable,
        )
      },
      extractSchema = if extractSchema != null then {
        throw Exception()
      } else null,
      extractAndValidateDataFrame = extractAndValidateDataFrame match {
        case null => null
        case other =>
          CoreTaskTemplateTransformationExtractAndValidateDataFrame(
            dataFrameIn = other.dataFrameIn,
            configLocation = other.configLocation,
          )
      },

      adHoc = if adHoc != null then {
        throw Exception()
      } else null,
    )
  }
}
