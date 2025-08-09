package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

case class CoreTaskTemplateTransformationSparkSql(
                                             sql: String,
                                             isLazyTransform: Boolean = false,
                                             lazyTable: List[String] = List.empty
                                           ) 