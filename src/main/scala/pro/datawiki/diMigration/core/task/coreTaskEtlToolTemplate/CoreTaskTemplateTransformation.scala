package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.{CoreTaskTemplateTransformationAdHoc, CoreTaskTemplateTransformationExtractAndValidateDataFrame, CoreTaskTemplateTransformationExtractSchema, CoreTaskTemplateTransformationIdMap, CoreTaskTemplateTransformationSparkSql}

case class CoreTaskTemplateTransformation(
                                           objectName: String,
                                           cache: String,
                                           idMap: CoreTaskTemplateTransformationIdMap,
                                           sparkSql: CoreTaskTemplateTransformationSparkSql,
                                           extractSchema: CoreTaskTemplateTransformationExtractSchema,
                                           extractAndValidateDataFrame: CoreTaskTemplateTransformationExtractAndValidateDataFrame,
                                           adHoc: CoreTaskTemplateTransformationAdHoc,
                                    ) {

}
