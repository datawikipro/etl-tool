package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.YamlDataEtlToolTemplate

import scala.collection.mutable

trait YamlDataTaskToolTemplate {
  def getCoreTask: List[CoreTask]
}

object YamlDataTaskToolTemplate {
  def apply(templateName:String, templateLocation:String, row: mutable.Map[String, String]):YamlDataTaskToolTemplate = {
    templateName match {
      case "EtlTool" => return YamlDataEtlToolTemplate(templateLocation, row)
      case _ => throw Exception()
    }
  }
  
}