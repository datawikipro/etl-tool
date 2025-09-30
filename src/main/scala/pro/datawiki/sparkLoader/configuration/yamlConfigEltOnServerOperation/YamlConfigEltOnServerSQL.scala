package pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.sparkLoader.configuration.YamlConfigEltOnServerOperationTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateElt, TaskTemplateSQLFromDatabase}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigEltOnServerSQL(
                                     sql: List[String],
                                   ) extends YamlConfigEltOnServerOperationTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    return TaskTemplateElt(sql, connection)
  }
}