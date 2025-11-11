package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateConnect
import pro.datawiki.sparkLoader.configuration.YamlConfigConnections

class YamlDataTemplateConnect(
                               sourceName: String,
                               connection: String,
                               configLocation: String,
                             ) {
  def getSourceName:String = sourceName
  def getCoreConnection: CoreTaskTemplateConnect = CoreTaskTemplateConnect(sourceName, connection, configLocation)
}