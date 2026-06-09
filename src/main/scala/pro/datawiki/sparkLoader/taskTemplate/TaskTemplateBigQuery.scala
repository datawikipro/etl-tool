package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.bigquery.LoaderBigQuery
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateBigQuery(
                            projectId: String,
                            datasetId: String,
                            tableId: String,
                            filter: String = null,
                            limit: Int = 0,
                            connection: ConnectionTrait
                          ) extends TaskTemplate with LoggingTrait {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("BigQuery table load", s"project: $projectId, dataset: $datasetId, table: $tableId")

    try {
      logInfo(s"Loading data from BigQuery table: $projectId.$datasetId.$tableId")
      logConfigInfo("BigQuery", s"filter: $filter, limit: $limit")

      connection match {
        case bigQueryLoader: LoaderBigQuery =>
          logInfo("Using BigQuery loader connection")
          val df: DataFrame = bigQueryLoader.readDfBigQuery(projectId, datasetId, tableId)

          val dataFrame = DataFrameOriginal(df)
          logOperationEnd("BigQuery table load", startTime, s"table: $tableId")
          return List(dataFrame)

        case _ =>
          logError("BigQuery load", new Exception(s"Expected LoaderBigQuery connection, but got ${connection.getClass.getName}"))
          throw new Exception(s"Expected LoaderBigQuery connection, but got ${connection.getClass.getName}")
      }

    } catch {
      case e: Exception =>
        logError("BigQuery table load", e, s"table: $projectId.$datasetId.$tableId")
        throw e
    }
  }
}