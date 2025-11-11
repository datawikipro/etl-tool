package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.coreTaskEtlToolTemplateTransformationIdmap

case class CoreTaskTemplateTransformationIdMapConfig(
                                                      systemCode: String,
                                                      columnNames: List[String],
                                                      domainName: String,
                                                      alias: String,
                                                      timeColumn: String,
                                                      secondForExpire: Int = -1,
                                                    )