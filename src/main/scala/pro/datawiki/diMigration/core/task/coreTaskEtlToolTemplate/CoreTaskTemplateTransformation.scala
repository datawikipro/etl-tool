package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.*

case class CoreTaskTemplateTransformation(
                                           objectName: String,
                                           idMap: CoreTaskTemplateTransformationIdMap,
                                           sparkSql: CoreTaskTemplateTransformationSparkSql,
                                           sparkSqlLazy: CoreTaskTemplateTransformationSparkSqlLazy,
                                           extractSchema: CoreTaskTemplateTransformationExtractSchema,
                                           extractAndValidateDataFrame: CoreTaskTemplateTransformationExtractAndValidateDataFrame,
                                           adHoc: CoreTaskTemplateTransformationAdHoc,
                                         ) {

}
