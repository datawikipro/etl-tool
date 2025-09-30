package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

case class CoreTaskTemplateTransformationSparkSqlLazy(
                                                   sql: String,
                                                   lazyTable: List[String] = List.empty
                                                 )