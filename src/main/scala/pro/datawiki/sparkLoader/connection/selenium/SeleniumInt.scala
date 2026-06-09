package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.exception.NotImplementedException

class SeleniumInt(in: Int) extends SeleniumType {
  override def getValue: Any = {
    throw NotImplementedException("getValue method not implemented for SeleniumInt")
  }
}
