package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.{YamlDataClickHouseForeignTableTemplate, YamlDataOdsMongoDBMirrorTemplate, YamlDataOdsPostgresMirrorTemplate}

import scala.collection.mutable

trait YamlDataToolTemplate {
  def getCoreDag: List[CoreDag]
}

object YamlDataToolTemplate {
  def apply(templateName:String, 
            templateLocation:String, 
            row: mutable.Map[String, String]):YamlDataToolTemplate = {
    templateName match {
      case "ClickHouseForeignTable" => return YamlDataClickHouseForeignTableTemplate(templateLocation, row)
      case "OdsPostgresMirror" => return YamlDataOdsPostgresMirrorTemplate(templateLocation, row)
      case "OdsMongoDBMirror" => return YamlDataOdsMongoDBMirrorTemplate(templateLocation, row)
      case _ => {
        throw Exception()
      }
    }
  }
  
}