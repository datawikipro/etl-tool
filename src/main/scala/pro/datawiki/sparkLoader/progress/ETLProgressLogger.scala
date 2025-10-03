package pro.datawiki.sparkLoader.progress

import pro.datawiki.exception.{ConfigurationException, DataProcessingException}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait

import java.time.LocalDateTime
import java.util.UUID

/**
 * Класс для логирования прогресса ETL процессов во внешнюю базу данных
 */
class ETLProgressLogger(
                         private val connection: DatabaseTrait,
                         private val tableName: String = "etl_progress_log"
                       ) extends LoggingTrait with ETLProgressLoggerTrait {

  private var currentProcess: Option[ETLProgressData] = None

  def initializeTable(): Unit = {
    val startTime = logOperationStart("initialize ETL progress table", s"table: $tableName")

    try {
      val createTableSQL =
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          process_id VARCHAR(100) PRIMARY KEY,
          process_name VARCHAR(255) NOT NULL,
          partition_name VARCHAR(100),
          config_location VARCHAR(500),
          start_time TIMESTAMP NOT NULL,
          end_time TIMESTAMP,
          status VARCHAR(50) NOT NULL,
          records_created BIGINT DEFAULT 0,
          records_updated BIGINT DEFAULT 0,
          records_deleted BIGINT DEFAULT 0,
          total_records_processed BIGINT DEFAULT 0,
          error_message TEXT,
          additional_info TEXT,
          dag_name VARCHAR(255) DEFAULT 'not defined',
          task_name VARCHAR(255) DEFAULT 'not defined',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      """

      connection.runSQL(createTableSQL)
      logInfo(s"ETL progress table initialized: $tableName")
      logOperationEnd("initialize ETL progress table", startTime)

    } catch {
      case e: Exception =>
        logError("initialize ETL progress table", e, s"table: $tableName")
        throw ConfigurationException(s"Failed to initialize ETL progress table: $tableName", e)
    }
  }

  def startProcess(
                    processName: String,
                    partitionName: String,
                    configLocation: String,
                    additionalInfo: Option[String] = None,
                    dagName: String = "not defined",
                    taskName: String = "not defined"
                  ): String = {
    val processId = UUID.randomUUID().toString
    val startTime = LocalDateTime.now()

    val progressData = ETLProgressData(
      processId = processId,
      processName = processName,
      partitionName = partitionName,
      configLocation = configLocation,
      startTime = startTime,
      status = ETLProgressStatus.Started,
      additionalInfo = additionalInfo,
      dagName = dagName,
      taskName = taskName
    )

    currentProcess = Some(progressData)
    saveProgressData(progressData)

    logInfo(s"Started ETL process: $processName (ID: $processId)")
    processId
  }

  def updateProgress(
                      processId: String,
                      operations: List[ETLOperationResult],
                      status: ETLProgressStatus = ETLProgressStatus.InProgress
                    ): Unit = {
    currentProcess match {
      case Some(process) if process.processId == processId =>
        val totalCreated = operations.filter(_.operationType == ETLOperationType.Create).map(_.recordsAffected).sum
        val totalUpdated = operations.filter(_.operationType == ETLOperationType.Update).map(_.recordsAffected).sum
        val totalDeleted = operations.filter(_.operationType == ETLOperationType.Delete).map(_.recordsAffected).sum
        val totalProcessed = operations.map(_.recordsAffected).sum

        val updatedProcess = process.copy(
          status = status,
          recordsCreated = process.recordsCreated + totalCreated,
          recordsUpdated = process.recordsUpdated + totalUpdated,
          recordsDeleted = process.recordsDeleted + totalDeleted,
          totalRecordsProcessed = process.totalRecordsProcessed + totalProcessed
        )

        currentProcess = Some(updatedProcess)
        saveProgressData(updatedProcess)

        logInfo(s"Updated ETL progress for process $processId: +$totalCreated created, +$totalUpdated updated, +$totalDeleted deleted")

      case Some(_) =>
        logWarning(s"Process ID mismatch: current process is not $processId")
      case None =>
        logWarning(s"No current process found for ID: $processId")
    }
  }

  def completeProcess(
                       processId: String,
                       status: ETLProgressStatus = ETLProgressStatus.Completed,
                       errorMessage: Option[String] = None
                     ): Unit = {
    currentProcess match {
      case Some(process) if process.processId == processId =>
        val completedProcess = process.copy(
          endTime = Some(LocalDateTime.now()),
          status = status,
          errorMessage = errorMessage
        )

        currentProcess = None
        saveProgressData(completedProcess)

        val duration = java.time.Duration.between(process.startTime, completedProcess.endTime.get).toMillis
        logInfo(s"Completed ETL process $processId with status $status in ${duration}ms")
        logInfo(s"Final stats - Created: ${completedProcess.recordsCreated}, Updated: ${completedProcess.recordsUpdated}, Deleted: ${completedProcess.recordsDeleted}")

      case Some(_) =>
        logWarning(s"Process ID mismatch: current process is not $processId")
      case None =>
        logWarning(s"No current process found for ID: $processId")
    }
  }

  private def saveProgressData(data: ETLProgressData): Unit = {
    try {
      val upsertSQL =
        s"""
        INSERT INTO $tableName (
          process_id, process_name, partition_name, config_location,
          start_time, end_time, status, records_created, records_updated,
          records_deleted, total_records_processed, error_message, additional_info,
          dag_name, task_name
        ) VALUES (
          '${data.processId}', '${data.processName}', '${data.partitionName}', 
          '${data.configLocation}', '${data.startTime}', 
          ${data.endTime.map(t => s"'$t'").getOrElse("NULL")}, 
          '${data.status}', ${data.recordsCreated}, ${data.recordsUpdated},
          ${data.recordsDeleted}, ${data.totalRecordsProcessed},
          ${data.errorMessage.map(msg => s"'${msg.replace("'", "''")}'").getOrElse("NULL")},
          ${data.additionalInfo.map(info => s"'${info.replace("'", "''")}'").getOrElse("NULL")},
          '${data.dagName}', '${data.taskName}'
        )
        ON CONFLICT (process_id) DO UPDATE SET
          end_time = EXCLUDED.end_time,
          status = EXCLUDED.status,
          records_created = EXCLUDED.records_created,
          records_updated = EXCLUDED.records_updated,
          records_deleted = EXCLUDED.records_deleted,
          total_records_processed = EXCLUDED.total_records_processed,
          error_message = EXCLUDED.error_message,
          additional_info = EXCLUDED.additional_info,
          dag_name = EXCLUDED.dag_name,
          task_name = EXCLUDED.task_name
      """

      connection.runSQL(upsertSQL)
      logDebug(s"Saved progress data for process: ${data.processId}")

    } catch {
      case e: Exception =>
        logError("save progress data", e, s"process: ${data.processId}")
      // Не прерываем выполнение ETL процесса из-за ошибок логирования
    }
  }

  def getCurrentProcess: Option[ETLProgressData] = currentProcess

  def close(): Unit = {
    if (currentProcess.isDefined) {
      logWarning("ETL progress logger closed with active process")
      currentProcess.foreach(process =>
        completeProcess(process.processId, ETLProgressStatus.Failed, Some("Process interrupted"))
      )
    }
  }
}

object ETLProgressLogger extends LoggingTrait {

  def apply(connection: ConnectionTrait, tableName: String = "etl_progress_log"): ETLProgressLogger = {
    connection match {
      case db: DatabaseTrait =>
        logInfo(s"Creating ETL progress logger with database connection")
        val logger = new ETLProgressLogger(db, tableName)
        logger.initializeTable()
        logger
      case _ =>
        val error = ConfigurationException("ETL progress logging requires a DatabaseTrait connection")
        logError("create ETL progress logger", error)
        throw error
    }
  }
}
