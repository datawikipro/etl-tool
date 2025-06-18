package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.SparkObject

import scala.collection.mutable

case class TaskTemplateAdHoc(sourceObjectName: String,
                             templateName: String,
                             columnId: List[String] = List.apply(),
                             asyncNumber: Int
                            ) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    if columnId.isEmpty then throw Exception()
    val sql = s"select ${columnId.mkString(",")} from $sourceObjectName"
    val df = SparkObject.spark.sql(sql)
    return List.apply(DataFrameOriginal(df))
  }
}
