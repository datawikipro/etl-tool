package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame
import org.openqa.selenium.WebElement
import pro.datawiki.exception.UnsupportedOperationException

object LogMode {
  var isDebug: Boolean = false

  def setDebug(in: Boolean): Unit = {
    isDebug = in
  }

  def debugString(in: String): Boolean = {
    if isDebug then
      println(in)
    return true
    // TODO: заменить на полноценное логирование
  }

  def getDebugFalse: Boolean = {
    if isDebug then
      throw UnsupportedOperationException("getDebugFalse not implemented")
    return false
  }

  def debugDF(df: DataFrame, df_name: String = ""): Unit = {
    try {
      if isDebug then {
        println(s"$df_name count_rows ${df.count()}")
        df.printSchema()
        df.show(20, false)
      }
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  def debugSelenium(webElement: WebElement): Unit = {
    if LogMode.isDebug then {
      //      println(
      //        s"""-----------------------------------------------------------------------------------
      //           |${webElement.getDomProperty("innerHTML")}
      //           |-----------------------------------------------------------------------------------""".stripMargin
      //      )
    }
  }

}
