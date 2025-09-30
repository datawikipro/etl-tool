package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.coreTaskEtlToolTemplateTransformationAdHoc

case class CoreTaskTemplateTransformationAdHocParameters(
                                                          parameterName: String,
                                                          columnName: String = "",
                                                          default: String = "",
                                                          decode: String = "None"
                                                        ) {

}