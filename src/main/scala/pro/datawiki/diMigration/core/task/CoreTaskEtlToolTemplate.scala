package pro.datawiki.diMigration.core.task

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.{CoreTaskTemplateConnect, CoreTaskTemplateSource, CoreTaskTemplateTarget, CoreTaskTemplateTransformation}
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}

case class CoreTaskEtlToolTemplate(
                                    taskName: String,
                                    yamlFile: String,
                                    connections: List[CoreTaskTemplateConnect] = List.apply(),
                                    sources: List[CoreTaskTemplateSource] = List.apply(),
                                    transform: List[CoreTaskTemplateTransformation] = List.apply(),
                                    target: List[CoreTaskTemplateTarget] = List.apply(),
                                    dependencies:List[String] = List.apply()
                                  ) extends CoreTask {


}