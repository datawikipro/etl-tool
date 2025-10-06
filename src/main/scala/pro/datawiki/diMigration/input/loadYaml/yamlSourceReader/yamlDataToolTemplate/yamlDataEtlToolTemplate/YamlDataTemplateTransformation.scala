package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateTransformation
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.{CoreTaskTemplateTransformationDeduplicate, CoreTaskTemplateTransformationExtractAndValidateDataFrame, CoreTaskTemplateTransformationExtractSchema, CoreTaskTemplateTransformationSparkSql, CoreTaskTemplateTransformationSparkSqlLazy}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.*
import pro.datawiki.exception.NotImplementedException

class YamlDataTemplateTransformation(
                                      objectName: String,
                                      cache: String,
                                      idMap: YamlDataTemplateTransformationIdMap,
                                      sparkSql: YamlDataTemplateTransformationSparkSql,
                                      sparkSqlLazy: YamlDataTemplateTransformationSparkSqlLazy,
                                      extractSchema: YamlDataTemplateTransformationExtractSchema,
                                      extractAndValidateDataFrame: YamlDataTemplateTransformationExtractAndValidateDataFrame,
                                      adHoc: YamlDataTemplateTransformationAdHoc,
                                      deduplicate: YamlDataTemplateTransformationDeduplicate,

                                    ) {
  def getCoreTransformation: CoreTaskTemplateTransformation = {
    CoreTaskTemplateTransformation(
      objectName = objectName,
      idMap = if idMap != null then {
        throw NotImplementedException(s"ID mapping transformation not implemented for object: $objectName")
      } else null,
      sparkSql = sparkSql match {
        case null => null
        case other => CoreTaskTemplateTransformationSparkSql(
          sql = other.sql
        )
      },
      sparkSqlLazy = sparkSqlLazy match {
        case null => null
        case other => CoreTaskTemplateTransformationSparkSqlLazy(
          sql = other.sql,
            lazyTable= other.lazyTable
        )
      },
      extractSchema = extractSchema match {
        case null => null
        case fs => CoreTaskTemplateTransformationExtractSchema(
          tableName = fs.tableName,
          jsonColumn = fs.jsonColumn,
          jsonResultColumn = fs.jsonResultColumn,
          baseSchema = fs.baseSchema,
          mergeSchema = fs.mergeSchema,
        )
      },
      extractAndValidateDataFrame = extractAndValidateDataFrame match {
        case null => null
        case other =>
          CoreTaskTemplateTransformationExtractAndValidateDataFrame(
            dataFrameIn = other.dataFrameIn,
            configLocation = other.configLocation,
          )
      },

      adHoc = if adHoc != null then {
        throw NotImplementedException(s"Ad-hoc transformation not implemented for object: $objectName")
      } else null,
      deduplicate = deduplicate match {
        case null => null
        case fs => CoreTaskTemplateTransformationDeduplicate(
          sourceTable = fs.sourceTable,
          uniqueKey = fs.uniqueKey,
          deduplicationKey = fs.deduplicationKey
        )
      }
    )
  }
}
