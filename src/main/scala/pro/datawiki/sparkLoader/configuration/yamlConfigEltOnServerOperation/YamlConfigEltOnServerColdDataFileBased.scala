package pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.sparkLoader.configuration.YamlConfigEltOnServerOperationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetFileSystem
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateColdFileBasedTableSimple, TaskTemplateElt, TaskTemplateSQLFromDatabase,TaskTemplateColdFileBasedTableHugeTable}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigEltOnServerColdDataFileBased(
                                                   source: YamlConfigEltOnServerColdDataFileBasedFolder,
                                                   target: YamlConfigEltOnServerColdDataFileBasedFolder,
                                                   extraColumn: List[YamlConfigEltOnServerColdDataFileBasedExtraColumn] = List.empty,
                                                   method: String,
                                                   withBackUp:Boolean = true,
                                                 ) extends YamlConfigEltOnServerOperationTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    method match {
      case "simple" => return TaskTemplateColdFileBasedTableSimple(source, target, connection)
      case "hugeTable" => return TaskTemplateColdFileBasedTableHugeTable(source, target,extraColumn,withBackUp, connection)
      case fs => {
        throw Exception()
      }
    }

  }
}