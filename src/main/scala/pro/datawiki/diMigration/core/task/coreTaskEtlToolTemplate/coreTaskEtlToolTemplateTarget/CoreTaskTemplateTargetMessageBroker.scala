package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

case class CoreTaskTemplateTargetMessageBroker(
                                                connection: String,
                                                source: String,
                                                mode: String = "append",
                                                target: String,
                                                partitionMode: String
                                              )