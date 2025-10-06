package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

case class CoreTaskTemplateTransformationDeduplicate(
                                                      sourceTable: String,
                                                      uniqueKey: List[String],
                                                      deduplicationKey: List[String]
                                                    )
