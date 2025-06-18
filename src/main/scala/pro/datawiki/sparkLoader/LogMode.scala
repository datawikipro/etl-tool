package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame
import org.openqa.selenium.WebElement

object LogMode {
  var isDebug: Boolean = false

  def setDebug(in: Boolean): Unit = {
    isDebug = in
  }
  def debugString(in:String): Boolean={
    if isDebug then
     println(in)
    return true
  }

  def getDebugFalse: Boolean = {
    if isDebug then
      throw Exception()
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

}
