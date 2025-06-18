package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

case class TaskTemplateIdMapMerge(sourceName:String,
                             connection: DatabaseTrait,
                             in:TaskTemplateIdMapConfig,
                             out:TaskTemplateIdMapConfig
                            ) extends TaskTemplate {
  val cache: TransformationCacheDatabase = TransformationCacheDatabase(connection)
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    val sql: String =
      s"""
         |select cast(${in.columnNames.mkString("!@#")}  as String) as in_ccd,
         |       cast(${out.columnNames.mkString("!@#")} as String) as out_ccd
         |  from ${sourceName}
         | where coalesce(${in.columnNames.mkString(",")}) is not null
         |   and coalesce(${out.columnNames.mkString(",")}) is not null
         | group by in_ccd, out_ccd
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)
    cache.saveTable(DataFrameOriginal(df), overwrite)
    connection.mergeIdMap(
      inTable =  cache.getLocation,
      domain = in.domainName,
      inSystemCode=in.systemCode,
      outSystemCode=out.systemCode
    )
    return List.empty
  }

}
