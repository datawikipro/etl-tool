package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

case class CoreTaskTemplateConnect(
                                    sourceName: String,
                                    connection: String,
                                    configLocation: String,
                                  )