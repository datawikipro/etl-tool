package pro.datawiki.sparkLoader.configuration

import pro.datawiki.exception.NotImplementedException

trait YamlConfigDataQuality {
  def a: Unit = throw NotImplementedException("Data quality validation not implemented")
}
