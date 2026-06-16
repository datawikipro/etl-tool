package pro.datawiki.sparkLoader.register

import pro.datawiki.sparkLoader.traits.LoggingTrait

trait TableRegisterTrait extends LoggingTrait {
  def registerTable(catalogName: String, schemaName: String, tableName: String, location: String): Unit
}
