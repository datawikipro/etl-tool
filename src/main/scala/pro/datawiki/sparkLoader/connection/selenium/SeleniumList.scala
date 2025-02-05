package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.sparkLoader.connection.selenium

class SeleniumList(in: List[KeyValue]) extends SeleniumType {
  var list: List[KeyValue] = in

  def appendElement(in: KeyValue): Unit = {
    list = list :+ in
  }

  def appendElements(in: SeleniumList): Unit = {
    list = list ::: in.getList
  }

  def getList:List[KeyValue] = list

  def getValueByKey(in:String): SeleniumType = {
    list.foreach(i=> if i.key == in then {
      return i.value
    })
    return null
  }
  
}

object SeleniumList {
  def apply():SeleniumList = {
    return new SeleniumList(List.apply())
  }

  def apply(in: List[KeyValue]): SeleniumList = {
    return new SeleniumList(in)
  }

  def applyByKeyValue(in: KeyValue): SeleniumList = {
    return new SeleniumList(List.apply(in))
  }

}