package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMapGenerate(sourceName: String,
                                dataAtServer: Boolean,
                                connection: DatabaseTrait,
                                template: TaskTemplateIdMapConfig
                               ) extends TaskTemplate {
  val cache: TransformationCacheDatabase = TransformationCacheDatabase()

  private def getTableFromSpark:String = {
    val sql: String =
      s"""
         |select ${template.columnNames.mkString("|| '!@#' ||")} as ccd
         |  from ${sourceName}
         | where nullif(coalesce(${template.columnNames.mkString(",")}),'') is not null
         | group by ccd
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)
    cache.saveTable(DataFrameOriginal(df), overwriteTable, connection)
    cache.getLocation
  }

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {


    connection match {
      case x: SupportIdMap =>
        var tableName = dataAtServer match {
          case true => x.createViewIdMapGenerate(sourceName,template.columnNames)
          case false => getTableFromSpark
        }
        x.generateIdMap(
          inTable = tableName,
          domain = template.domainName,
          systemCode = template.systemCode
        )
      case _ => throw UnsupportedOperationException("Unsupported connection type for ID map generation")
    }
    return List.empty
  }

}
