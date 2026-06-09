package pro.datawiki.sparkLoader.task

import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.context.TaskContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.sparkLoader.transformation.TransformationCache

class TaskAdHocRegister(inTaskTemplate: TaskTemplate) extends Task {
  def setSkipIfEmpty(in: Boolean): Unit = throw UnsupportedOperationException("setSkipIfEmpty not implemented in TaskAdHocRegister")
  
  override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    TaskContext.setTaskTemplate(targetName, inTaskTemplate)
    return ProgressStatus.done
  }


}
