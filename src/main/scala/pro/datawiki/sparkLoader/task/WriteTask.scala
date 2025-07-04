package pro.datawiki.sparkLoader.task

import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class WriteTask(inTaskTemplate: TaskTemplate) extends Task {
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }
  override def run(targetName: String, parameters: mutable.Map[String, String], isSync:Boolean): ProgressStatus = {
    throw Exception()
  }

  override def setCache(in: TransformationCacheTrait): Unit = throw Exception()
}
