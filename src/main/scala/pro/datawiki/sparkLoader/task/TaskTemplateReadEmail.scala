package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.mail.LoaderMail

import java.time.LocalDateTime
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
      case other => throw new DataProcessingException(s"Неизвестный тип вложения email: '$other'")
  }

}