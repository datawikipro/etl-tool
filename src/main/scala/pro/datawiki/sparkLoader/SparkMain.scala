package pro.datawiki.sparkLoader

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.configuration.EltConfig
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.progress.ETLProgressStatus
import pro.datawiki.sparkLoader.traits.LoggingTrait

@main
def sparkRun(args: String*): Unit = {
  SparkMain.run(args)
}

object SparkMain extends LoggingTrait {

  /**
   * Setup silent mode - suppresses all logging output.
   * Called when debug=false to ensure clean stdout for parsing.
   */
  def setupSilentMode(): Unit = {
    // Suppress log4j warnings
    System.setProperty("log4j.threshold", "OFF")
    
    // Set all major loggers to OFF
    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com.clickhouse").setLevel(Level.OFF)
    Logger.getLogger("org.sparkproject").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("io.netty").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF)
    Logger.getLogger("org.mongodb").setLevel(Level.OFF)
    Logger.getLogger("com.mongodb").setLevel(Level.OFF)
    Logger.getLogger("ch.qos.logback").setLevel(Level.OFF)
  }

  /**
   * TEMPORARY: Copies the YAML config file to debug_output folder for debugging purposes.
   * Creates the debug_output directory if it doesn't exist.
   */
  def copyYamlToDebug(configLocation: String, runId: String): Unit = {
    try {
      val sourceFile = new File(configLocation)
      if (sourceFile.exists()) {
        // Create debug_output directory in project root
        val debugDir = new File("debug_output")
        if (!debugDir.exists()) {
          debugDir.mkdirs()
        }
        
        // Create filename with timestamp and runId
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
        val originalName = sourceFile.getName
        val debugFileName = s"${timestamp}_${runId}_$originalName"
        val destPath = Paths.get(debugDir.getAbsolutePath, debugFileName)
        
        // Copy file
        Files.copy(sourceFile.toPath, destPath, StandardCopyOption.REPLACE_EXISTING)
        logInfo(s"[DEBUG] Copied YAML config to: ${destPath.toAbsolutePath}")
      } else {
        logWarning(s"[DEBUG] Cannot copy YAML config - source file not found: $configLocation")
      }
    } catch {
      case e: Exception =>
        logWarning(s"[DEBUG] Failed to copy YAML config to debug_output: ${e.getMessage}")
    }
  }

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
  
  def run(args: Seq[String]): Unit = {
    val startTime = logOperationStart("spark ETL process", s"args: ${args.mkString(" ")}")
    var exitCode = 0
    var shouldExit = false

    try {
      val config: Config = SparkRunCLI.parseArgs(args.toArray)
      val externalLoggingInfo = config.externalProgressLogging match {
        case Some(externalConfig) => s", external_progress_logging: ${externalConfig.connection}@${externalConfig.configLocation}"
        case None => ""
      }
      logConfigInfo("spark ETL", s"config: ${config.configLocation}, partition: ${config.partition}, debug: ${config.isDebug}$externalLoggingInfo")

      try {
        LogMode.setDebug(config.isDebug)
        
        // Suppress all logging when not in debug mode
//        if (!config.isDebug) {
//          setupSilentMode()
//        }
        
        ApplicationContext.setRunId(config.runId)
        ApplicationContext.setGlobalVariable("partition",config.partition)
        ApplicationContext.setGlobalVariable("load_date",config.loadDate.replace("-",""))//TODO

        // TEMPORARY: Copy YAML config to debug_output for debugging
        copyYamlToDebug(config.configLocation, config.runId)

        val etlProcessId: Option[String] = initLoging(config,args.toList)

        EltConfig(config.configLocation) match {
          case ProgressStatus.done =>
            logInfo("ETL process completed successfully")
            etlProcessId.foreach(processId => completeETLProcess(processId, ETLProgressStatus.Completed))
            exitCode = 0
            shouldExit = true
          case ProgressStatus.skip =>
            logInfo("ETL process skipped")
            etlProcessId.foreach(processId => completeETLProcess(processId, ETLProgressStatus.Skipped))
            exitCode = 2
            shouldExit = true
          case ProgressStatus.error =>
            throw DataProcessingException("ETL process failed with error status")
          case _ =>
            throw DataProcessingException()
        }
      } catch {
        case e: org.apache.spark.sql.streaming.StreamingQueryException => { //TODO
          logError("ETL process skipped", e)
          exitCode = 2
          shouldExit = true
        }
      } finally {
        logInfo("Closing connections")
        ApplicationContext.closeConnections()

        // Stop Spark session to release resources and daemon threads cleanly
        if (SparkObject.localSpark != null) {
          try {
            logInfo("Stopping Spark session")
            SparkObject.localSpark.stop()
          } catch {
            case e: Exception => logWarning(s"Failed to stop Spark session: ${e.getMessage}")
          }
        }

        // Закрываем ETL progress logger
        getETLProgressLogger.foreach(_.close())
      }
    } catch {
      case e: Exception =>
        logError("spark ETL process", e, "unexpected error occurred")
        exitCode = 1
        shouldExit = true
    } finally {
      logOperationEnd("spark ETL process", startTime)
    }

    if (shouldExit) {
      System.exit(exitCode)
    }
  }
}