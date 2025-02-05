package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.Row

class SeleniumArray(in: List[SeleniumList]) extends SeleniumType {
  var list: List[SeleniumList] = in

  def appendElement(in: SeleniumList): Unit = {
    list = list :+ in
  }

  def appendElements(in: SeleniumArray): Unit = {
    list = list ::: in.getList
  }
  
  def getList:List[SeleniumList] = list
  
}

object SeleniumArray {
  def apply():SeleniumArray = {
    return new SeleniumArray(List.apply())
  }
}
