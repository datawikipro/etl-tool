package pro.datawiki.sparkLoader.progress

/**
 * Тип операции ETL
 */
enum ETLOperationType {
  case Create, Update, Delete, Merge, Truncate, Other
}
