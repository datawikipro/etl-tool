package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataByTemplate.YamlDataByTemplateSources

import scala.collection.mutable


case class YamlDataByTemplate(
                               dagNameTemplate: String,
                               templateType: String,
                               templateLocation: String,
                               nameTemplate: List[String],
                               sources: List[YamlDataByTemplateSources] = List.apply(),
                             ) extends YamlDataToolTemplate {

  override def getCoreDag: List[CoreDag] = {
    var list: List[CoreDag] = List.apply()
    var row: mutable.Map[String, String] = mutable.Map()

    nameTemplate.zipWithIndex.foreach {
      case (value, index) => {
        row += (s"dataTemplate[${index}]" -> value)
      }
    }

    sources.foreach(i => {
      var row2: mutable.Map[String, String] = row
      i.nameTemplate.zipWithIndex.foreach {
        case (value, index) => {
          row2 += (s"dataSourceTemplate[${index}]" -> value)
        }
      }
      YamlDataToolTemplate(templateType, templateLocation, row).getCoreDag.foreach(i => list = list.appended(i))
    })

    return list
  }

}