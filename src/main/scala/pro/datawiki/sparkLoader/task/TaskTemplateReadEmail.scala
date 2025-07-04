package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.mail.LoaderMail
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import java.time.LocalDateTime
import java.util.{Calendar, Date}
import scala.collection.mutable

class TaskTemplateReadEmail(email:String,
                            password:String,
                            from:String,
                            subject: String,
                            connection: ConnectionTrait) extends TaskTemplate {

  var time: LocalDateTime = null

  def setTime(in: LocalDateTime): Unit = {
    time = in
  }
  
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    var df: DataFrame = null
    connection match
      case x: LoaderMail =>
        val df = x.getDataFrame(email,password,from,subject,time)
        return List.apply(df)
      case _ => throw Exception()
  }

}