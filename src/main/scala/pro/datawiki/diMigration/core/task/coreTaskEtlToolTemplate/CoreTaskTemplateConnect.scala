package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.sparkLoader.configuration.YamlConfigConnections

case class CoreTaskTemplateConnect(
                                    sourceName: String,
                                    connection: String,
                                    configLocation: String,
                                  ) {
  def getEtToolFormat: YamlConfigConnections = YamlConfigConnections(
    sourceName = sourceName,
    connection = connection,
    configLocation = configLocation
  )
}