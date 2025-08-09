package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.{Task, TaskTemplate}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

trait YamlConfigSourceTrait {
  @JsonIgnore
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate

}
