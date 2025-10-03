package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

case class CoreTaskTemplateTransformationExtractSchema(
                                                        tableName: String,
                                                        jsonColumn: String,
                                                        jsonResultColumn: String,
                                                        baseSchema: String,
                                                        mergeSchema: Boolean
                                                      )