package pro.datawiki.sparkLoader.task

import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskAdHocRegister(inTaskTemplate: TaskTemplate) extends Task {

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync:Boolean): Boolean = {
    Context.setTaskTemplate(targetName, inTaskTemplate)
    return true
  }

  override def setCache(in: TransformationCacheTrait): Unit = throw Exception()

}
