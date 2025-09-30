package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

case class CoreTaskTemplateTransformationAdHoc(
                                                sourceObjectName: String,
                                                templateName: String,
                                                columnId: List[String] = List.apply(),
                                                asyncNumber: Int
                                              )