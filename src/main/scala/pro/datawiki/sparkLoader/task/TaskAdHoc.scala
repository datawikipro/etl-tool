package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.task.taskAdHoc.{TaskAdHocParameter, TaskAdHocParameters}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateAdHoc}

import java.util.concurrent.Executors
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

class TaskAdHoc(adHocTemplate: TaskTemplateAdHoc, inTaskTemplate: TaskTemplate) extends Task {

  var isSkipIfEmpty: Boolean = false

  def setSkipIfEmpty(in: Boolean): Unit = {
    isSkipIfEmpty = in
  }

  private def getParameters(row: Row): TaskAdHocParameters = {
    TaskAdHocParameters(row.schema.map(i => TaskAdHocParameter(i.name, s"${row.get(row.fieldIndex(i.name))}")).toList)
  }

  private def getParametersFromDataFrame(df: DataFrameTrait): List[TaskAdHocParameters] = {
    if !df.isValidData then return List.apply()
    val l = df.getDataFrame.collect().map(j => getParameters(j)).toList
    return l
  }

  private def threadLogic(in: TaskAdHocParameters, isSync: Boolean): DataFrameTrait = {
    val treadDf = inTaskTemplate.run(in.getMap, isSync)
    treadDf.length match
      case 1 => return treadDf.head
      case _ => throw UnsupportedOperationException("Unsupported number of DataFrames for AdHoc task")
  }

  override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    val dfList: List[DataFrameTrait] = adHocTemplate.run(parameters, isSync)
    val list: List[TaskAdHocParameters] = dfList.flatMap(getParametersFromDataFrame)
    LogMode.isDebug match {
      case true => SparkContext.saveDf(targetName, runSync(list))
      case false => SparkContext.saveDf(targetName, runAsync(list))
    }
    return ProgressStatus.done
  }

  def mergeDataframe(in: List[DataFrameTrait]): DataFrameTrait = {
    return in.reduce((df1, df2) => df1.unionAll(df2))
  }

  def mergeDataframes(in: List[DataFrameTrait]): List[DataFrameTrait] = {
    // Логируем начало операции
    logInfo(s"TaskAdHoc.mergeDataframes: начинается объединение ${in.length} DataFrame")
    
    val listPartitions = in.map(col => col.getPartitionName).distinct
    logInfo(s"TaskAdHoc.mergeDataframes: найдено ${listPartitions.length} уникальных партиций")
    
    val result = listPartitions.zipWithIndex.map { case (col, index) =>
      val filteredDataFrames = in.filter(col1 => col1.getPartitionName == col)
      
      // Логируем прогресс каждые 100 элементов
      if ((index + 1) % 100 == 0) {
        logInfo(s"TaskAdHoc.mergeDataframes: обработано ${index + 1} из ${listPartitions.length} партиций")
      }
      
      mergeDataframe(filteredDataFrames)
    }

    // Логируем завершение операции
    logInfo(s"TaskAdHoc.mergeDataframes: завершено объединение, получено ${result.length} DataFrame")
    
    return result
  }

  def runSync(list: List[TaskAdHocParameters]): List[DataFrameTrait] = {
    // Логируем начало обработки
    logInfo(s"TaskAdHoc: начинается синхронная обработка ${list.length} элементов")
    
    try {
      val results = list.zipWithIndex.map { case (element, index) =>
        try {
          val result = threadLogic(element, true)
          
          // Логируем прогресс каждые 100 элементов
          if ((index + 1) % 100 == 0) {
            logInfo(s"TaskAdHoc: обработано ${index + 1} из ${list.length} элементов")
          }
          
          Some(result)
        } catch {
          case e: Exception => {
            logError(s"TaskAdHoc: Ошибка при синхронной обработке элемента ${index + 1}: ${e.getMessage}", e)
            None
          }
        }
      }
      logInfo(s"TaskAdHoc: обработано ${list.length} из ${list.length} элементов")
      // Фильтруем успешно обработанные результаты
      val successfulResults = results.flatten.filterNot(col => col.isEmpty)
      val failedCount = results.count(_.isEmpty)

      // Логируем статистику обработки
      logInfo(s"TaskAdHoc: завершена синхронная обработка. Успешно: ${successfulResults.length}, Ошибок: ${failedCount} из ${list.length} элементов")

      // Проверяем условие: если обработалось >0, продолжаем, если 0 - ошибка
      if (successfulResults.isEmpty) {
        val errorMsg = s"TaskAdHoc: Критическая ошибка - не удалось обработать ни одного элемента из ${list.length}"
        logError("TaskAdHoc sync processing", new RuntimeException(errorMsg), errorMsg)
        throw new RuntimeException(errorMsg)
      }

      return mergeDataframes(successfulResults)

    } catch {
      case e: Exception => {
        logError(s"TaskAdHoc: Общая ошибка при синхронной обработке: ${e.getMessage}", e)
        throw e
      }
    }
  }


  def runAsync(list: List[TaskAdHocParameters]): List[DataFrameTrait] = {
    // Логируем начало обработки
    logInfo(s"TaskAdHoc: начинается асинхронная обработка ${list.length} элементов")
    
    var numThreads = adHocTemplate.asyncNumber
    if list.length < numThreads then numThreads = list.length
    logInfo(s"TaskAdHoc: используется ${numThreads} потоков для обработки")
    
    val executorService = Executors.newFixedThreadPool(numThreads)

    // Создаем ExecutionContext на основе ExecutorService
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

    try {
      // Создаем Future для каждого элемента массива с отслеживанием прогресса
      val futures: List[Future[Option[DataFrameTrait]]] = list.zipWithIndex.map { case (element, index) =>
        Future {
          try {
            val result = threadLogic(element, false)
            
            // Логируем прогресс каждые 100 элементов
            if ((index + 1) % 100 == 0) {
              logInfo(s"TaskAdHoc: обработано ${index + 1} из ${list.length} элементов")
            }
            
            Some(result)
          } catch {
            case e: java.util.concurrent.RejectedExecutionException => {
              logError(s"TaskAdHoc async processing element ${index + 1}", e, s"RejectedExecutionException для элемента ${index + 1}")
              None
            }
            case e: Exception => {
              logError(s"TaskAdHoc: Ошибка при обработке элемента ${index + 1}: ${e.getMessage}", e)
              None
            }
          }
        }
      }

      // Ожидаем завершения всех Future и собираем результаты
      val resultFuture = Future.sequence(futures)

      // Синхронно ожидаем результат
      val results = Await.result(resultFuture, 18000.seconds)

      // Фильтруем успешно обработанные результаты
      val successfulResults = results.flatten.filterNot(col => col.isEmpty)
      val failedCount = results.count(_.isEmpty)

      // Логируем статистику обработки
      logInfo(s"TaskAdHoc: завершена обработка. Успешно: ${successfulResults.length}, Ошибок: ${failedCount} из ${list.length} элементов")

      // Проверяем условие: если обработалось >0, продолжаем, если 0 - ошибка
      if (successfulResults.isEmpty) {
        val errorMsg = s"TaskAdHoc: Критическая ошибка - не удалось обработать ни одного элемента из ${list.length}"
        logError("TaskAdHoc async processing", new RuntimeException(errorMsg), errorMsg)
        throw new RuntimeException(errorMsg)
      }

      return mergeDataframes(successfulResults)

    } catch {
      case e: java.util.concurrent.RejectedExecutionException => {
        logError(s"TaskAdHoc: RejectedExecutionException при создании Future: ${e.getMessage}", e)
        throw e
      }
      case e: Exception => {
        logError(s"TaskAdHoc: Общая ошибка при асинхронной обработке: ${e.getMessage}", e)
        throw e
      }
    } finally {
      executorService.shutdown()
    }
  }
}
