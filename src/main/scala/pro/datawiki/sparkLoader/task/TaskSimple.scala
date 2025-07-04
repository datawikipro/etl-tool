package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.configuration.ProgressStatus.{error, skip}
import pro.datawiki.sparkLoader.connection.WriteMode
import pro.datawiki.sparkLoader.exception.TableNotExistException
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskSimple(inTaskTemplate: TaskTemplate) extends Task {
  var cache: TransformationCacheTrait = null
  var isSkipIfEmpty: Boolean = false
  def setSkipIfEmpty(in:Boolean): Unit = {
    isSkipIfEmpty = in
  }
  def runLogic(targetName: String, parameters: mutable.Map[String, String], isSync:Boolean): ProgressStatus={
    var df: List[DataFrameTrait] = inTaskTemplate.run(parameters = parameters, true)
    if cache != null then {
      if df.length != 1 then throw Exception()
      df.head match
        case x: DataFrameOriginal => {
          cache.saveTable(x, WriteMode.overwrite)
          return Task.saveDf(targetName, cache.readDirty())
        }
        case _ => throw Exception()
    }

    Task.saveDf(targetName, df)

  }

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync:Boolean): ProgressStatus = {
    if LogMode.isDebug then return runLogic(targetName, parameters, isSync)
    try {
      return runLogic(targetName, parameters, isSync)
    } catch {
      case e: TableNotExistException => {
        if isSkipIfEmpty then return skip
        return error
      }
      case e:Exception => throw e
      case _ => throw Exception()
    }

  }

  override def setCache(in: TransformationCacheTrait): Unit = {
    cache = in
  }
}
