package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameConsumer, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException, TableNotExistException}
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.configuration.ProgressStatus.skip
import pro.datawiki.sparkLoader.connection.WriteMode
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskConsumer(inTaskTemplate: TaskTemplate) extends Task {
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
        return ProgressStatus.skip
      }
      case e: Exception => throw e
    }
    if cache == null then return Task.saveDf(targetName, df)

    df.length match {
      case 0 => return ProgressStatus.skip
      case 1 => {
        df.head match
          case x: DataFrameOriginal => {
            cache.saveTable(x, WriteMode.overwrite)
            return Task.saveDf(targetName, cache.readDirty())
          }
          case _ => throw new DataProcessingException(s"Expected exactly one DataFrame, got: ${df.length}")
      }
      case _ => throw new DataProcessingException(s"Expected exactly one DataFrame, got: ${df.length}")
    }
  }

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    Task.saveDf(targetName, DataFrameConsumer(null))
    new Thread(() => {
      var status: ProgressStatus = null
      while (status != ProgressStatus.done) {
        status = runLogic(targetName, parameters, isSync)
      }
    }).start()
    return ProgressStatus.done
  }

  override def setCache(in: TransformationCacheTrait): Unit = {
    cache = in
  }
}
