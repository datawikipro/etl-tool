package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.functions.col
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateTableFromFileSystem(tableName: String,
                                      partitionBy: List[String] = List.apply(),
                                      where: String,
                                      limit: Int, source: FileStorageTrait) extends TaskTemplate with LoggingTrait {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("file system table load", s"table: $tableName")

    try {
      logInfo(s"Loading data from file system table: $tableName")
      logConfigInfo("file system table", s"partitions: ${partitionBy.length}, where: $where, limit: $limit")

      var df = source.readDf(tableName, partitionBy, ApplicationContext.getPartitions(partitionBy *))

      // Проверяем наличие поврежденных записей (для JSON файлов)
      if (df.columns.contains("_corrupt_record")) {
        val corruptCount = df.filter(col("_corrupt_record").isNotNull).count()
        if (corruptCount > 0) {
          logWarning(s"Found $corruptCount corrupted JSON records in table: $tableName")
        }
      }

      if where != null then {
        logInfo(s"Applying WHERE filter: $where")
        df = df.where(where)
        val filteredCount = df.count()
      }

      if limit > 0 then {
        logInfo(s"Applying LIMIT: $limit")
        df = df.limit(limit)
      } 
      logOperationEnd("file system table load", startTime, s"table: $tableName")

      return List.apply(DataFrameOriginal(df))
    } catch {
      case e: Exception =>
        logError("file system table load", e, s"table: $tableName")
        throw e
    }
  }

}
