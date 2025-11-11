package pro.datawiki.diMigration.core.task

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.*


case class CoreTaskEtlToolTemplate(
                                    taskName: String,
                                    yamlFile: String,
                                    connections: List[CoreTaskTemplateConnect],
                                    preEtlOperations: List[CoreTaskTemplateEltOnServerOperation],
                                    sources: List[CoreTaskTemplateSource],
                                    transform: List[CoreTaskTemplateTransformation],
                                    target: List[CoreTaskTemplateTarget],
                                    postEtlOperations: List[CoreTaskTemplateEltOnServerOperation],
                                    dependencies: List[String]
                                  ) extends CoreTask {


  override def getTaskId: String = taskName
}