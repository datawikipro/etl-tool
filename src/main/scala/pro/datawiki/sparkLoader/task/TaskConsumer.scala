package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameConsumer, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{DataProcessingException, NotImplementedException, TableNotExistException}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus.skip
import pro.datawiki.sparkLoader.dictionaryEnum.{ProgressStatus, WriteMode}
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.sparkLoader.transformation.TransformationCache

import scala.collection.mutable

class TaskConsumer(inTaskTemplate: TaskTemplate,skipIfEmpty: Boolean) extends Task {
//  var cache: TransformationCache = null
//  var connection: ConnectionTrait = null
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  def runLogic(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = { 
    throw NotImplementedException("runLogic not implemented for TaskConsumer")
    //    var df: List[DataFrameTrait] = List.apply()
//    try {
//      df = inTaskTemplate.run(parameters = parameters, true)
//    } catch {
//      case e: TableNotExistException => {
//        return ProgressStatus.skip
//      }
//      case e: Exception => throw e
//    }
//    if cache == null then return Task.saveDf(targetName, df)
//
//    df.length match {
//      case 0 => return ProgressStatus.skip
//      case 1 => {
//        df.head match
//          case x: DataFrameOriginal => {
//            cache.saveTable(x, WriteMode.overwriteTable,connection)
//            return Task.saveDf(targetName, cache.readDirty(connection))
//          }
//          case _ => throw DataProcessingException(s"Expected exactly one DataFrame, got: ${df.length}")
//      }
//      case _ => throw DataProcessingException(s"Expected exactly one DataFrame, got: ${df.length}")
//    }
  }

  override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    throw NotImplementedException("run not implemented for TaskConsumer")
    
//    Task.saveDf(targetName, DataFrameConsumer(null))
//    new Thread(() => {
//      var status: ProgressStatus = null
//      while (status != ProgressStatus.done) {
//        status = runLogic(targetName, parameters, isSync)
//      }
//    }).start()
//    return ProgressStatus.done
  }

}
