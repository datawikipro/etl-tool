package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate

trait YamlConfigSourceTrait {
  @JsonIgnore
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate

}
