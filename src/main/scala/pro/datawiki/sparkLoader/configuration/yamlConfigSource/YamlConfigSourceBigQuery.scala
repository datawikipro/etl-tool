package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateBigQuery}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceBigQuery( //TODO удалить из проекта
                                     projectId: String,
                                     datasetId: String,
                                     tableId: String,
                                     filter: String = null,
                                     limit: Int = 0
                                   ) extends YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    // Иначе читаем всю таблицу с возможными фильтрами
    return TaskTemplateBigQuery(projectId, datasetId, tableId = tableId, filter = filter, limit = limit, connection = connection)

  }

}
