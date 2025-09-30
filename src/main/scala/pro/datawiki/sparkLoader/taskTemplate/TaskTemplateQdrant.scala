package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.qdrant.LoaderQdrant

class TaskTemplateQdrant(
                          collectionName: String,
                          connection: ConnectionTrait
                        ) extends TaskTemplate {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    connection match {
      case qdrantLoader: LoaderQdrant =>
        val df: DataFrame = qdrantLoader.readDf(collectionName)
        val dataFrame = DataFrameOriginal(df)
        return List(dataFrame)
      case _ =>
        throw new Exception(s"Expected LoaderQdrant connection, but got ${connection.getClass.getName}")
    }
  }
}
