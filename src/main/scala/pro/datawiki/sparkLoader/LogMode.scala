package pro.datawiki.sparkLoader

import org.apache.spark.sql.DataFrame

object LogMode {
  var isDebug: Boolean = false
  def setDebug(in:Boolean):Unit = {
    isDebug = in
  }

  def getDebugFalse:Boolean={
    if isDebug then
      throw Exception()
    return false
  }
  
  def debugDF(df:DataFrame):Unit={
    if isDebug then {
      df.printSchema()
      df.show()
    }
  }
  
  
  
  
}
