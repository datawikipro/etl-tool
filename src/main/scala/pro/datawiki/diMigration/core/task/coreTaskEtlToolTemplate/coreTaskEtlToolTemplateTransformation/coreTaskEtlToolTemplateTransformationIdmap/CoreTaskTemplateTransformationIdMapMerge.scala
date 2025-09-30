package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.coreTaskEtlToolTemplateTransformationIdmap

case class CoreTaskTemplateTransformationIdMapMerge(
                                                     alias: String,
                                                     restoreRk: Boolean = false,
                                                     in: CoreTaskTemplateTransformationIdMapBaseConfig,
                                                     out: CoreTaskTemplateTransformationIdMapBaseConfig
                                                   )