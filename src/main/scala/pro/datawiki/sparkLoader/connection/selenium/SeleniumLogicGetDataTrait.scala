package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.exception.NotImplementedException

trait SeleniumLogicGetDataTrait {
  def getData: Boolean = throw NotImplementedException("getData method not implemented for SeleniumLogicGetDataTrait")
}
