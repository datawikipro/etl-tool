package pro.datawiki.sparkLoader.progress

/**
 * Trait для ETL progress logger, чтобы избежать циклических зависимостей
 */
trait ETLProgressLoggerTrait {
  def startProcess(processName: String, partitionName: String, configLocation: String, additionalInfo: Option[String] = None, dagName: String = "not defined", taskName: String = "not defined"): String

  def updateProgress(processId: String, operations: List[ETLOperationResult], status: ETLProgressStatus = ETLProgressStatus.InProgress): Unit

  def completeProcess(processId: String, status: ETLProgressStatus = ETLProgressStatus.Completed, errorMessage: Option[String] = None): Unit

  def getCurrentProcess: Option[ETLProgressData]

  def close(): Unit
}
