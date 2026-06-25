package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.connection.minIo.minioIceberg.LoaderMinIoIceberg
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

case class TaskTemplateIdMapMerge(sourceName: String,
                                  connection: ConnectionTrait with SupportIdMap,
                                  dataAtServer:Boolean,
                                  in: TaskTemplateIdMapConfig,
                                  out: TaskTemplateIdMapConfig
                                 ) extends TaskTemplate {

  private def getTableFromSpark: String = {
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

    connection match {
      case db: DatabaseTrait =>
        val cache = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df), overwriteTable, db)
        cache.getLocation
      case fs: LoaderMinIoIceberg =>
        val tempViewName = "tmp_idmap_merge_spark_" + scala.util.Random.alphanumeric.filter(_.isLetter).take(10).mkString
        df.createOrReplaceTempView(tempViewName)
        tempViewName
      case _ =>
        throw UnsupportedOperationException(s"Unsupported connection type for ID map merge: ${connection.getClass.getSimpleName}")
    }
  }
  
  override def run(parameters:Map[String, String], isSync: Boolean): List[DataFrameTrait] = {

    connection match {
      case x: SupportIdMap => {
        var tableName = dataAtServer match {
          case true => x.createViewIdMapMerge(sourceName, in.columnNames,out.columnNames)
          case false => getTableFromSpark
        }
        x.mergeIdMap(
          inTable = tableName,
          domain = in.domainName,
          inSystemCode = in.systemCode,
          outSystemCode = out.systemCode
        )
      }
      case _ => throw UnsupportedOperationException("Unsupported connection type for ID map merge")
    }
    return List.empty
  }

}
