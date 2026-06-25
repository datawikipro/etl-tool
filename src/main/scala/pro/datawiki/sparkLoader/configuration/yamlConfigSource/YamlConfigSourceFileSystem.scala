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
                                       tableLocation: Option[String] = None,
                                       tableColumns: List[YamlConfigSourceDBTableColumn] = List.empty,
                                       partitionBy: List[String] = List.empty,
                                       where: String = null,
                                       limit: Int = 0
                                     ) extends YamlConfigSourceTrait {
  @JsonIgnore
  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    val resolvedLocation = tableLocation.getOrElse(getDefaultLocation(tableName))
    connection match
      case x: FileStorageTrait => 
        return TaskTemplateTableFromFileSystem(
          tableName = tableName, 
          tableLocation = resolvedLocation,
          partitionBy = partitionBy, 
          where = where, 
          limit = limit, 
          source = x
        )
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for FileSystem source: ${connection.getClass.getSimpleName}")
  }

  private def getDefaultLocation(name: String): String = {
    if (name.contains('/')) {
      name
    } else {
      val lastDot = name.lastIndexOf('.')
      if (lastDot != -1) {
        val schema = name.substring(0, lastDot)
        val table = name.substring(lastDot + 1)
        s"$schema/$table"
      } else {
        name
      }
    }
  }

}