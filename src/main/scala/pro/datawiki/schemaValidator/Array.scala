package pro.datawiki.schemaValidator

import org.json4s.JValue


case class Array(
                  `arrayElement`: Object
                ) {
  def processArray(x: List[JValue]): Unit = {
    x.foreach(i => `arrayElement`.validateJson(i))
  }
}
