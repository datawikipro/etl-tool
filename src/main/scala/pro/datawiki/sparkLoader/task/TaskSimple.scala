package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.TableNotExistException
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus.skip
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate

class TaskSimple(inTaskTemplate: TaskTemplate) extends Task {
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  def runLogic(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    var df: List[DataFrameTrait] = List.apply()
    try {
      df = inTaskTemplate.run(parameters = parameters, true)
    } catch {
      case e: TableNotExistException => {
        if isSkipIfEmpty then return skip
        throw e
      }
      case e: Exception => {
        throw e
      }
    }

    SparkContext.saveDf(targetName, df)
  }

  override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    if LogMode.isDebug then return runLogic(targetName, parameters.toMap, isSync)
    try {
      return runLogic(targetName, parameters.toMap, isSync)
    } catch {
      case e: Exception => {
        throw e
      }
    }

  }
  
}
