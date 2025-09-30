package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTransformation.coreTaskEtlToolTemplateTransformationIdmap.{CoreTaskTemplateTransformationIdMapConfig, CoreTaskTemplateTransformationIdMapMerge}

case class CoreTaskTemplateTransformationIdMap(
                                                sourceName: String,
                                                connection: String,
                                                idMapGenerate: List[CoreTaskTemplateTransformationIdMapConfig] = List.apply(),
                                                idMapRestore: List[CoreTaskTemplateTransformationIdMapConfig] = List.apply(),
                                                idMapMerge: List[CoreTaskTemplateTransformationIdMapMerge] = List.apply()
                                              )