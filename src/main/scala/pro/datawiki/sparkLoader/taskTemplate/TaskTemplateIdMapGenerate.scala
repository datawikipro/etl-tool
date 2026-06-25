package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.connection.minIo.minioIceberg.LoaderMinIoIceberg
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMapGenerate(sourceName: String,
                                dataAtServer: Boolean,
                                connection: ConnectionTrait with SupportIdMap,
                                template: TaskTemplateIdMapConfig
                               ) extends TaskTemplate {

  private def getTableFromSpark: String = {
    val sql: String =
      s"""
         |select ${template.columnNames.mkString("|| '!@#' ||")} as ccd
         |  from ${sourceName}
         | where nullif(coalesce(${template.columnNames.mkString(",")}),'') is not null
         | group by ccd
         |  """.stripMargin

    var df = SparkObject.spark.sql(sqlText = sql)
    LogMode.debugDF(df)

    connection match {
      case db: DatabaseTrait =>
        val cache = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df), overwriteTable, db)
        cache.getLocation
      case fs: LoaderMinIoIceberg =>
        val tempViewName = "tmp_idmap_gen_spark_" + scala.util.Random.alphanumeric.filter(_.isLetter).take(10).mkString
        df.createOrReplaceTempView(tempViewName)
        tempViewName
      case _ =>
        throw UnsupportedOperationException(s"Unsupported connection type for ID map generation: ${connection.getClass.getSimpleName}")
    }
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
          systemCode = template.systemCode,
          tableLocation = template.getResolvedLocation
        )
      case _ => throw UnsupportedOperationException("Unsupported connection type for ID map generation")
    }
    return List.empty
  }

}
