package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateTableFromFileSystem}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceFileSystem(
                                       tableName: String,
                                       tableColumns: List[YamlConfigSourceDBTableColumn],
                                       partitionBy: List[String] = List.apply(),
                                       where: String,
                                       limit: Int
                                     ) extends YamlConfigSourceTrait {
  @JsonIgnore
  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    connection match
      case x: FileStorageTrait => return TaskTemplateTableFromFileSystem(tableName = tableName, partitionBy = partitionBy, where = where, limit = limit, source = x)
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for FileSystem source: ${connection.getClass.getSimpleName}")
  }

}