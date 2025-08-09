package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMapGenerate(sourceName: String,
                                connection: DatabaseTrait,
                                template: TaskTemplateIdMapConfig
                               ) extends TaskTemplate {
  val cache: TransformationCacheDatabase = TransformationCacheDatabase(connection)

  override def run(parameters: mutable.Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val sql: String =
      s"""
         |select ${template.columnNames.mkString("!@#")} as ccd 
         |  from ${sourceName}
         | where nullif(coalesce(${template.columnNames.mkString(",")}),'') is not null
         | group by ccd
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)
    cache.saveTable(DataFrameOriginal(df), overwrite)
    connection match {
      case x: SupportIdMap =>

        x.generateIdMap(
          inTable = cache.getLocation,
          domain = template.domainName,
          systemCode = template.systemCode
        )
      case _ => throw Exception()
    }
    return List.empty
  }

}
