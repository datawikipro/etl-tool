package pro.datawiki.sparkLoader.connection.selenium

import scala.collection.mutable

class SeleniumArray(in: List[Map[String, SeleniumType]]) extends SeleniumType {
    def getList: List[Map[String, SeleniumType]] = in

    override def getValue: Any = {
        throw Exception()
    }
}