package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.ValidationException
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.jsonApi.ValidateStatus

case class TaskTemplateAdHoc(sourceObjectName: String,
                             templateName: String,
                             columnId: List[String],
                             asyncNumber: Int
                            ) extends TaskTemplate {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    if columnId.isEmpty then throw new ValidationException("columnId cannot be empty")
    val sql = s"select ${columnId.mkString(",")} from $sourceObjectName"
    val df: DataFrame = SparkObject.spark.sql(sql)
    return List.apply(new DataFrameOriginal(df))
  }
}
