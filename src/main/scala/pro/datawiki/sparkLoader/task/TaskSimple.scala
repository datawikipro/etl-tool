package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, TableNotExistException}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.configuration.ProgressStatus.{error, skip}
import pro.datawiki.sparkLoader.connection.WriteMode
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskSimple(inTaskTemplate: TaskTemplate) extends Task {
  var cache: TransformationCacheTrait = null
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  def runLogic(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    var df: List[DataFrameTrait] = List.apply()
    try {
      df = inTaskTemplate.run(parameters = parameters, true)
    } catch {
      case e: TableNotExistException => {
        if isSkipIfEmpty then return skip
        throw e
      }
      case e: DataProcessingException => {
        throw e // Пробрасываем ошибки обработки данных как есть
      }
      case e: Exception => {
        throw new DataProcessingException(s"Ошибка при выполнении шаблона задачи: ${e.getMessage}", e)
      }
    }
    if cache != null then {
      if df.length != 1 then throw new DataProcessingException(s"Expected exactly one DataFrame, got: ${df.length}")
      df.head match
        case x: DataFrameOriginal => {
          cache.saveTable(x, WriteMode.overwrite)
          return Task.saveDf(targetName, cache.readDirty())
        }
        case _ => throw new DataProcessingException("Expected DataFrameOriginal")
    }

    Task.saveDf(targetName, df)
  }

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    if LogMode.isDebug then return runLogic(targetName, parameters, isSync)
    try {
      return runLogic(targetName, parameters, isSync)
    } catch {
      case e: TableNotExistException => {
        if isSkipIfEmpty then return skip
        LogMode.error(s"Таблица не существует: ${e.getMessage}")
        return error
      }
      case e: DataProcessingException => {
        LogMode.error(s"Ошибка обработки данных: ${e.getMessage}")
        throw e
      }
      case e: Throwable => {
        LogMode.error(s"Непредвиденная ошибка при выполнении задачи: ${e.getMessage}")
        throw new DataProcessingException(s"Непредвиденная ошибка при выполнении задачи: ${e.getMessage}", e)
      }
    }

  }

  override def setCache(in: TransformationCacheTrait): Unit = {
    cache = in
  }
}
