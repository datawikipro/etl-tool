package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.{TaskTemplate, TaskTemplateSQLFromDatabase}

case class YamlConfigSourceDBSQL(
                                  sql: String,
                                ) extends YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    return TaskTemplateSQLFromDatabase(sql, connection)
  }
}