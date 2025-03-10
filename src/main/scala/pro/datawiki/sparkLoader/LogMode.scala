package pro.datawiki.sparkLoader

object LogMode {
  var isDebug: Boolean = false
  def setDebug(in:Boolean):Unit = {
    isDebug = in
  } 
  
}
