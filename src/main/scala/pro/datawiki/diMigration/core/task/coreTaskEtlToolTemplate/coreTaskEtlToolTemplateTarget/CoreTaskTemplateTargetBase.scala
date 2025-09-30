package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

class CoreTaskTemplateTargetBase(connection: String,
                                 mode: String = "append",
                                 partitionMode: String,
                                 source: String
                                )