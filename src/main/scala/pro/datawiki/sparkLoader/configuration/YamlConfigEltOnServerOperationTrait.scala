package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.Task
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.sparkLoader.transformation.TransformationCache

trait YamlConfigEltOnServerOperationTrait {
  @JsonIgnore
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate

}
