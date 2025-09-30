package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

case class CoreTaskTemplateSourceMail(
                                       email: String,
                                       password: String,
                                       from: String,
                                       subject: String,
                                     )