package pro.datawiki.sparkLoader.progress

/**
 * Статус выполнения ETL процесса
 */
enum ETLProgressStatus {
  case Started, InProgress, Completed, Failed, Skipped
}