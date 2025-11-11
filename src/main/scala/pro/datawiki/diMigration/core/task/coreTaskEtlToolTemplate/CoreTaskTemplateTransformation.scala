package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.*
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformation
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.*
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.{YamlConfigTransformationIdMapBaseConfig, YamlConfigTransformationIdMapConfig, YamlConfigTransformationIdMapMerge}
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressMode

case class CoreTaskTemplateTransformation(
                                           objectName: String,
                                           idMap: CoreTaskTemplateTransformationIdMap,
                                           sparkSql: CoreTaskTemplateTransformationSparkSql,
                                           sparkSqlLazy: CoreTaskTemplateTransformationSparkSqlLazy,
                                           extractSchema: CoreTaskTemplateTransformationExtractSchema,
                                           extractAndValidateDataFrame: CoreTaskTemplateTransformationExtractAndValidateDataFrame,
                                           adHoc: CoreTaskTemplateTransformationAdHoc,
                                           deduplicate: CoreTaskTemplateTransformationDeduplicate,
                                         ) {
  def getEtToolFormat: YamlConfigTransformation = YamlConfigTransformation(
    objectName = objectName,
    idMap = idMap match {
      case null => null
      case fs =>
        YamlConfigTransformationIdMap(
          sourceName = fs.sourceName,
          connection = fs.connection,
          dataAtServer = fs.dataAtServer,
          idMapGenerate = fs.idMapGenerate.map(col =>
            YamlConfigTransformationIdMapConfig(
              systemCode = col.systemCode,
              columnNames = col.columnNames,
              domainName = col.domainName,
              alias = col.alias,
              timeColumn = col.timeColumn,
              secondForExpire = col.secondForExpire
            )
          ),
          idMapRestore = fs.idMapRestore.map(col =>
            YamlConfigTransformationIdMapConfig(
              systemCode = col.systemCode,
              columnNames = col.columnNames,
              domainName = col.domainName,
              alias = col.alias,
              timeColumn = col.timeColumn,
              secondForExpire = col.secondForExpire
            )
          ),
          idMapMerge = fs.idMapMerge.map(col =>
            YamlConfigTransformationIdMapMerge(
              alias = col.alias,
              restoreRk = col.restoreRk,
              in =
                YamlConfigTransformationIdMapBaseConfig(
                  systemCode = col.in.systemCode,
                  columnNames = col.in.columnNames,
                  domainName = col.in.domainName,
                  timeColumn = col.in.timeColumn,
                  secondForExpire = col.in.secondForExpire,
                ),
              out =
                YamlConfigTransformationIdMapBaseConfig(
                  systemCode = col.out.systemCode,
                  columnNames = col.out.columnNames,
                  domainName = col.out.domainName,
                  timeColumn = col.out.timeColumn,
                  secondForExpire = col.out.secondForExpire,
                )
            )
          ))
    },
    sparkSql = sparkSql match {
      case null => null
      case fs =>
        YamlConfigTransformationSparkSql(sql = fs.sql)
    },
    sparkSqlLazy = sparkSqlLazy match {
      case null => null
      case fs =>
        YamlConfigTransformationSparkSqlLazy(
          sql = fs.sql,
          lazyTable = fs.lazyTable)
    },

    extractSchema = extractSchema match {
      case null => null
      case fs =>
        YamlConfigTransformationExtractSchema(
          tableName = fs.tableName,
          jsonColumn = fs.jsonColumn,
          jsonResultColumn = fs.jsonResultColumn,
          baseSchema = fs.baseSchema,
          mergeSchema = fs.mergeSchema,
          loadMode = ProgressMode.batch.toString2
        )
    },
    extractAndValidateDataFrame = extractAndValidateDataFrame match {
      case null => null
      case fs =>
        YamlConfigTransformationExtractAndValidateDataFrame(
          dataFrameIn = fs.dataFrameIn,
          configLocation = fs.configLocation,
        )
    },
    adHoc = adHoc match {
      case null => null
      case fs =>
        YamlConfigTransformationAdHoc(
          sourceObjectName = fs.sourceObjectName,
          templateName = fs.templateName,
          columnId = fs.columnId,
          asyncNumber = fs.asyncNumber
        )
    },
    deduplicate = deduplicate match {
      case null => null
      case fs =>
        YamlConfigTransformationDeduplicate(
          sourceTable = fs.sourceTable,
          uniqueKey = fs.uniqueKey,
          deduplicationKey = fs.deduplicationKey
        )
    }
  )
}
