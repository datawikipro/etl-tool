package pro.datawiki.sparkLoader.task

import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskAdHocRegister(inTaskTemplate: TaskTemplate) extends Task {
  def setSkipIfEmpty(in: Boolean): Unit = throw new UnsupportedOperationException("setSkipIfEmpty not implemented in TaskAdHocRegister")

  private var cache: TransformationCacheTrait = null
  private var isSkipIfEmpty: Boolean = false

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    Context.setTaskTemplate(targetName, inTaskTemplate)
    return ProgressStatus.done
  }

  override def setCache(in: TransformationCacheTrait): Unit = throw new UnsupportedOperationException("setCache not implemented in TaskAdHocRegister")

}
