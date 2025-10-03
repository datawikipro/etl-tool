package pro.datawiki.sparkLoader.traits

import ch.qos.logback.classic.{Level, Logger}
import com.fasterxml.jackson.annotation.JsonInclude
import com.google.cloud.spark.bigquery.repackaged.io.openlineage.spark.shade.com.fasterxml.jackson.annotation.JsonIgnore
import org.slf4j.LoggerFactory
import pro.datawiki.sparkLoader.progress.{ETLOperationResult, ETLOperationType, ETLProgressLoggerTrait, ETLProgressStatus}

/**
 * Trait для логирования в модуле sparkLoader
 * Предоставляет единообразный доступ к логгеру для всех классов
 */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
trait LoggingTrait {
  @JsonIgnore
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)

  @JsonIgnore
  protected def logOperationStart(operation: String, details: String = ""): Long = {
    val timestamp = System.currentTimeMillis()
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.info(s"Starting $message")
    return timestamp
  }

  @JsonIgnore
  protected def logOperationEnd(operation: String, startTime: Long, details: String = ""): Unit = {
    val duration = System.currentTimeMillis() - startTime
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.info(s"Completed $message in ${duration}ms")
  }

  @JsonIgnore
  protected def logError(operation: String, error: Throwable, details: String = ""): Unit = {
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.error(s"Failed $message", error)
  }

  protected def throwError(operation: String, error: Throwable, details: String = ""): Nothing = {
    logError(operation, error, details)
    throw error
  }
  
  @JsonIgnore
  protected def logWarning(message: String): Unit = {
    logger.warn(message)
  }

  @JsonIgnore
  protected def logInfo(message: String): Unit = {
    logger.info(message)
  }

  @JsonIgnore
  protected def logDebug(message: String): Unit = {
    logger.debug(message)
  }

  @JsonIgnore
  protected def setLogLevel(level: Level): Unit = {
    logger.asInstanceOf[Logger].setLevel(level)
  }

  @JsonIgnore
  protected def logSparkOperation(operation: String, details: String = ""): Unit = {
    val message = if (details.nonEmpty) s"Spark $operation: $details" else s"Spark $operation"
    logger.info(message)
  }

  @JsonIgnore
  protected def logConfigInfo(configName: String, details: String = ""): Unit = {
    val message = if (details.nonEmpty) s"Config $configName: $details" else s"Config $configName"
    logger.info(message)
  }

  // Переменная для хранения экземпляра ETL progress logger
  @JsonIgnore
  private var etlProgressLogger: Option[ETLProgressLoggerTrait] = None

  @JsonIgnore
  protected def setETLProgressLogger(logger: ETLProgressLoggerTrait): Unit = {
    etlProgressLogger = Some(logger)
    logDebug("ETL progress logger has been set")
  }

  @JsonIgnore
  protected def logETLProgress(processId: String,
                               operations: List[ETLOperationResult],
                               status: ETLProgressStatus = ETLProgressStatus.InProgress): Unit = {
    etlProgressLogger match {
      case Some(logger) =>
        logger.updateProgress(processId, operations, status)
        val totalCreated = operations.filter(_.operationType == ETLOperationType.Create).map(_.recordsAffected).sum
        val totalUpdated = operations.filter(_.operationType == ETLOperationType.Update).map(_.recordsAffected).sum
        val totalDeleted = operations.filter(_.operationType == ETLOperationType.Delete).map(_.recordsAffected).sum
        logInfo(s"ETL progress logged: $totalCreated created, $totalUpdated updated, $totalDeleted deleted")
      case None =>
        logDebug("ETL progress logger not configured, skipping external logging")
    }
  }


  @JsonIgnore
  protected def startETLProcess(
                                 processName: String,
                                 partitionName: String,
                                 configLocation: String,
                                 additionalInfo: Option[String] = None,
                                 dagName: String = "not defined",
                                 taskName: String = "not defined"
                               ): Option[String] = {
    etlProgressLogger match {
      case Some(logger) =>
        val processId = logger.startProcess(processName, partitionName, configLocation, additionalInfo, dagName, taskName)
        logInfo(s"Started ETL process: $processName (ID: $processId) [DAG: $dagName, Task: $taskName]")
        Some(processId)
      case None =>
        logDebug("ETL progress logger not configured, skipping external process tracking")
        None
    }
  }

  @JsonIgnore
  protected def completeETLProcess(
                                    processId: String,
                                    status: ETLProgressStatus = ETLProgressStatus.Completed,
                                    errorMessage: Option[String] = None
                                  ): Unit = {
    etlProgressLogger match {
      case Some(logger) =>
        logger.completeProcess(processId, status, errorMessage)
        logInfo(s"Completed ETL process: $processId with status $status")
      case None =>
        logDebug("ETL progress logger not configured, skipping external process completion")
    }
  }

  @JsonIgnore
  protected def getETLProgressLogger: Option[ETLProgressLoggerTrait] = etlProgressLogger
}
