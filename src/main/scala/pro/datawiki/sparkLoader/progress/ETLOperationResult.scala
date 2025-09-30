package pro.datawiki.sparkLoader.progress

/**
 * Операция ETL для отслеживания изменений
 */
case class ETLOperationResult(
                               operationType: ETLOperationType,
                               recordsAffected: Long,
                               tableName: Option[String] = None,
                               details: Option[String] = None
                             )
