package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSQLFromDatabase}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceDBSQL(
                                  sql: String,
                                ) extends YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    return TaskTemplateSQLFromDatabase(sql, connection)
  }
}