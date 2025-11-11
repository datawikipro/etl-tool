package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.coreTaskEtlToolTemplateTransformationIdmap

case class CoreTaskTemplateTransformationIdMapBaseConfig(
                                                          systemCode: String,
                                                          columnNames: List[String],
                                                          domainName: String,
                                                          timeColumn: String,
                                                          secondForExpire: Int = -1,
                                                        )