package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.ValidationException
import pro.datawiki.sparkLoader.SparkObject

import scala.collection.mutable

case class TaskTemplateAdHoc(sourceObjectName: String,
                             templateName: String,
                             columnId: List[String] = List.apply(),
                             asyncNumber: Int
                            ) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    if columnId.isEmpty then throw new ValidationException("columnId cannot be empty")
    val sql = s"select ${columnId.mkString(",")} from $sourceObjectName"
    val df: DataFrame = SparkObject.spark.sql(sql)
    return List.apply(new DataFrameOriginal(df))
  }
}
