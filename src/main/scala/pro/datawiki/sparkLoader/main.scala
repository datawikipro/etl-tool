package pro.datawiki.sparkLoader

import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.configuration.{EltConfig}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.progress.ETLProgressStatus
import pro.datawiki.sparkLoader.traits.LoggingTrait

object SparkMain extends LoggingTrait {

  def initLoging(config: Config,args:List[String]): Option[String]={
    // Инициализация внешнего логирования прогресса ETL, если настроено
    val etlProcessId: Option[String] = config.externalProgressLogging match {
      case Some(externalConfig) =>
        try {
          logInfo(s"Initializing external ETL progress logging: ${externalConfig.connection}")
          val progressConnection = ConnectionTrait(
            "etl_progress_logger",
            externalConfig.connection,
            externalConfig.configLocation
          )
          // Создаем ETL progress logger через reflection чтобы избежать циклических зависимостей
          val progressLoggerClass = Class.forName("pro.datawiki.sparkLoader.progress.ETLProgressLogger")
          val constructor = progressLoggerClass.getConstructor(
            Class.forName("pro.datawiki.sparkLoader.connection.DatabaseTrait"),
            classOf[String]
          )
          val progressLogger = constructor.newInstance(
            progressConnection.asInstanceOf[Object],
            "etl_progress_log"
          ).asInstanceOf[pro.datawiki.sparkLoader.progress.ETLProgressLoggerTrait]

          // Инициализируем таблицу
          val initMethod = progressLoggerClass.getMethod("initializeTable")
          initMethod.invoke(progressLogger)
          setETLProgressLogger(progressLogger)

          // Начинаем отслеживание ETL процесса
          val processId = startETLProcess(
            processName = "spark ETL process",
            partitionName = config.partition,
            configLocation = config.configLocation,
            additionalInfo = Some(s"args: ${args.mkString(" ")}"),
            dagName = config.dagName,
            taskName = config.taskName
          )
          logInfo(s"Started external ETL progress tracking with ID: ${processId.getOrElse("N/A")}")
          processId

        } catch {
          case e: Exception =>
            logError("initialize external ETL progress logging", e, "continuing without external logging")
            None
        }
      case None =>
        logDebug("External ETL progress logging not configured")
        None
    }
    return etlProcessId
  }
  
  @main
  def sparkRun(args: String*): Unit = {
    val startTime = logOperationStart("spark ETL process", s"args: ${args.mkString(" ")}")

    try {
      val config: Config = SparkRunCLI.parseArgs(args.toArray)
      val externalLoggingInfo = config.externalProgressLogging match {
        case Some(externalConfig) => s", external_progress_logging: ${externalConfig.connection}@${externalConfig.configLocation}"
        case None => ""
      }
      logConfigInfo("spark ETL", s"config: ${config.configLocation}, partition: ${config.partition}, debug: ${config.isDebug}$externalLoggingInfo")

      try {
        LogMode.setDebug(config.isDebug)
        ApplicationContext.setRunId(config.runId)
        ApplicationContext.setGlobalVariable("partition",config.partition)
        ApplicationContext.setGlobalVariable("load_date",config.loadDate.replace("-",""))//TODO

        val etlProcessId: Option[String] = initLoging(config,args.toList)

        val result: ProgressStatus = EltConfig(config.configLocation) match {
          case ProgressStatus.done =>
            logInfo("ETL process completed successfully")
            etlProcessId.foreach(processId => completeETLProcess(processId, ETLProgressStatus.Completed)            )
            return
          case ProgressStatus.skip =>
            logInfo("ETL process skipped")
            etlProcessId.foreach(processId => completeETLProcess(processId, ETLProgressStatus.Skipped)            )
            return
          case ProgressStatus.error =>
            throw DataProcessingException("ETL process failed with error status")
          case _ =>
            throw DataProcessingException()
        }
      } catch {
        case e: Exception =>
          throw e
      } finally {
        logInfo("Closing connections")
        ApplicationContext.closeConnections()
        // Закрываем ETL progress logger
        getETLProgressLogger.foreach(_.close())
      }
    } catch {
      case e: Exception =>
        logError("spark ETL process", e, "unexpected error occurred")
        System.exit(1)
    } finally {
      logOperationEnd("spark ETL process", startTime)
    }
  }
}