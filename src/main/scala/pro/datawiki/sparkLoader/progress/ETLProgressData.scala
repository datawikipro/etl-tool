package pro.datawiki.sparkLoader.progress

import java.time.LocalDateTime

/**
 * Модель данных для логирования прогресса ETL процессов
 */
case class ETLProgressData(
                            processId: String,
                            processName: String,
                            partitionName: String,
                            configLocation: String,
                            startTime: LocalDateTime,
                            endTime: Option[LocalDateTime] = None,
                            status: ETLProgressStatus,
                            recordsCreated: Long = 0L,
                            recordsUpdated: Long = 0L,
                            recordsDeleted: Long = 0L,
                            totalRecordsProcessed: Long = 0L,
                            errorMessage: Option[String] = None,
                            additionalInfo: Option[String] = None,
                            dagName: String = "not defined",
                            taskName: String = "not defined"
                          )



