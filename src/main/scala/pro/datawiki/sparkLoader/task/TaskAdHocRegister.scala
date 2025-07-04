package pro.datawiki.sparkLoader.task

import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskAdHocRegister(inTaskTemplate: TaskTemplate) extends Task {
  def setSkipIfEmpty(in: Boolean): Unit = throw Exception()

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    Context.setTaskTemplate(targetName, inTaskTemplate)
    return ProgressStatus.done
  }

  override def setCache(in: TransformationCacheTrait): Unit = throw Exception()

}
