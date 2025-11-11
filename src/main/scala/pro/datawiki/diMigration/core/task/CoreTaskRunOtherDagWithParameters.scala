package pro.datawiki.diMigration.core.task

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.*


case class CoreTaskRunOtherDagWithParameters(
                                    taskName: String,
                                    runnableDagName: String,
                                    parameters: Map[String,String]        
                                  ) extends CoreTask {
  
  override def getTaskId: String = taskName
}