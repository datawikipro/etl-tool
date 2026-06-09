package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.mail.LoaderMail
import pro.datawiki.sparkLoader.traits.LoggingTrait

import java.time.LocalDateTime
import scala.collection.mutable

class TaskTemplateReadEmail(email: String,
                            password: String,
                            from: String,
                            subject: String,
                            connection: ConnectionTrait) extends TaskTemplate with LoggingTrait {

  var time: LocalDateTime = null

  def setTime(in: LocalDateTime): Unit = {
    time = in
  }

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("read email", s"from: $from, subject: $subject")

    try {
      logInfo(s"Reading email from: $from with subject: $subject")
      logConfigInfo("email", s"from: $from, subject: $subject, sync: $isSync")

      connection match
        case x: LoaderMail =>
          logInfo("Using LoaderMail connection for email reading")
          val df = x.getDataFrame(email, password, from, subject, time)
          logOperationEnd("read email", startTime, s"from: $from, subject: $subject")
          return List.apply(df)
        case other =>
          logError("read email", DataProcessingException(s"Неизвестный тип вложения email: '$other'"))
          throw DataProcessingException(s"Неизвестный тип вложения email: '$other'")

    } catch {
      case e: Exception =>
        logError("read email", e, s"from: $from, subject: $subject")
        throw e
    }
  }

}