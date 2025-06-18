package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateSQLFromDatabase(sql:String,
                                  connection: ConnectionTrait) extends TaskTemplate {

  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    var df: DataFrame = null
    connection match
      case x: DatabaseTrait =>
        val df = DataFrameOriginal(x.getDataFrameBySQL(s"""${sql}""".stripMargin))
        return List.apply(df)
      case _ => throw Exception()
  }

}