package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame
import org.openqa.selenium.WebElement
import pro.datawiki.exception.UnsupportedOperationException

object LogMode {
  var isDebug: Boolean = false

  def setDebug(in: Boolean): Unit = {
    isDebug = in
  }
  def debugString(in:String): Boolean={
    if isDebug then
     println(in)
    return true
    // TODO: заменить на полноценное логирование
  }

  def getDebugFalse: Boolean = {
    if isDebug then
      throw new UnsupportedOperationException("getDebugFalse not implemented")
    return false
  }

  def debugDF(df: DataFrame,df_name: String=""): Unit = {
    if isDebug then {
      println(df_name)
      df.printSchema()
//      df.show()
      df.show(20, false)
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
  def error(in: String): Unit = {
    println(in)
  }
}
