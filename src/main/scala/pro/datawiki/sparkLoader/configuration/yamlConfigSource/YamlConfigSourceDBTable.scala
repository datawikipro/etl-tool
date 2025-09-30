package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.{ YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateTableFromDatabase}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    filter: String,
                                    limit: Int) extends YamlConfigSourceTrait {
  @JsonIgnore
  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    return new TaskTemplateTableFromDatabase(tableSchema, tableName, tableColumns, filter, limit, connection)
  }

}