package pro.datawiki.sparkLoader.task

import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.sparkLoader.transformation.TransformationCache

import scala.collection.mutable

class WriteTask(inTaskTemplate: TaskTemplate) extends Task {
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    throw UnsupportedOperationException("WriteTask.run not implemented")
  }
  
}
