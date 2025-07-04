package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.ProgressStatus
import pro.datawiki.sparkLoader.connection.WriteMode.append
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}


class TaskAdHoc(adHocTemplate: TaskTemplateAdHoc, inTaskTemplate: TaskTemplate) extends Task {

  var cache: TransformationCacheTrait = null
  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  private def getParameters(row: Row): mutable.Map[String, String] = {
    var parameters: mutable.Map[String, String] = mutable.Map()
    row.schema.foreach(i => {
      parameters += (i.name, row.get(row.fieldIndex(i.name)).toString)
    })
    return parameters
  }

  private def getParametersFromDataFrame(df: DataFrameTrait): List[mutable.Map[String, String]] = {
    var list: List[mutable.Map[String, String]] = List.apply()
    if !df.isValidData then return list

    df.getDataFrame.collect().foreach(j => {
      list = list.appended(getParameters(j))
    })
    return list
  }

  private def threadLogicSub(in: mutable.Map[String, String], isSync: Boolean): Boolean = {
    val treadCache = cache.createNewCache
    val treadDf = inTaskTemplate.run(in, isSync)
    treadDf.length match
      case 1 => treadCache.saveTable(treadDf.head, append)
      case _ => throw Exception()
    cache.append(treadCache)
    return true
  }

  private def threadLogic(in: mutable.Map[String, String], isSync: Boolean): Boolean = {
    if LogMode.isDebug then return threadLogicSub(in, isSync)

    try {
      threadLogicSub(in, isSync)
    } catch
      case e: Exception => {
        println(e)
        return false
      }
  }

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync: Boolean): ProgressStatus = {
    val df: List[DataFrameTrait] = adHocTemplate.run(parameters, isSync)
    var list: List[mutable.Map[String, String]] = List.apply()
    df.foreach(i => {
      list = list ::: getParametersFromDataFrame(i)
    })

    if LogMode.isDebug then {
      runSync(list)
    } else {
      adHocTemplate.asyncNumber match {
        case 1 => runSync(list)
        case _ => runAsync(list)
      }
    }

    Task.saveDf(targetName, cache.readDirty())
    return ProgressStatus.done
  }

  def runSync(list: List[mutable.Map[String, String]]): Unit = {
    list.foreach(element => threadLogic(element, true))
  }

  def runAsync(list: List[mutable.Map[String, String]]): Unit = {
    //      // Количество потоков
    var numThreads = adHocTemplate.asyncNumber
    if list.length < numThreads then numThreads = list.length
    //
    // Создаем ExecutorService с фиксированным пулом из 8 потоков
    val executorService = Executors.newFixedThreadPool(numThreads)

    // Создаем ExecutionContext на основе ExecutorService
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

    // Создаем Future для каждого элемента массива
    val futures = list.map { element =>
      Future {
        threadLogic(element, false)
      }
    }
    // Ожидаем завершения всех Future и собираем результаты
    val resultFuture = Future.sequence(futures)

    // Обработка результатов
    resultFuture.onComplete {
      case Success(results) =>
      case Failure(e) =>
        println(s"Произошла ошибка: ${e.getMessage}")
    }

    // Ожидаем завершения всех Future (для демонстрации)
    Await.ready(resultFuture, 18000.seconds)
    executorService.shutdown()
    //    if skipped > 0 then println(s"skipped ${skipped}")
  }


  override def setCache(in: TransformationCacheTrait): Unit = {
    cache = in
  }

}
