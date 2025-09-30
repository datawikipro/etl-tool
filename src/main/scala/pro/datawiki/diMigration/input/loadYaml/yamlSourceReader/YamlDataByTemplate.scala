package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataByTemplate.YamlDataByTemplateSources
import pro.datawiki.textVariable.WorkWithText

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable


case class YamlDataByTemplate(
                               dagName: String,
                               dagNameTemplate: String,
                               pythonFile: String = throw Exception("pythonFile must be initialized"),
                               description: String,
                               templateLogic: List[TemplateLogic],
                               nameTemplate: List[String],
                               sources: List[YamlDataByTemplateSources] = List.apply(),
                               schedule: String = throw Exception(),
                               startDate: Option[String] = None,
                               catchup: Boolean = false,
                             ) extends YamlDataToolTemplate {

  private def parseStartDate(startDateStr: Option[String]): Date = {
    startDateStr match {
      case Some(dateStr) if dateStr.nonEmpty => {
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        formatter.parse(dateStr)
      }
      case _ => {
        // If startDate is null or empty, return null
        null
      }
    }
  }

  override def getCoreDag: List[CoreDag] = {
    var list: List[CoreDag] = List.apply()
    var variablesSourceLevel: mutable.Map[String, String] = mutable.Map()

    nameTemplate.zipWithIndex.foreach {
      case (value, index) => {
        variablesSourceLevel += (s"dataTemplate[${index}]" -> value)
      }
    }

    sources.foreach(i => {
      var variablesDagLevel: mutable.Map[String, String] = variablesSourceLevel
      i.nameTemplate.zipWithIndex.foreach {
        case (value, index) => {
          variablesDagLevel += (s"dataSourceTemplate[${index}]" -> value)
        }
      }
      

      list = list.appended(CoreBaseDag(
        dagName = WorkWithText.replaceWithoutDecode(dagName, variablesDagLevel),
        tags = List.apply("ods") ::: nameTemplate,
        dagDescription = WorkWithText.replaceWithoutDecode(description, variablesDagLevel),
        schedule = Schedule(schedule),
        startDate = parseStartDate(startDate),
        catchup = catchup,
        pythonFile = WorkWithText.replaceWithoutDecode(pythonFile, variablesDagLevel),
        taskPipelines = templateLogic.map(rw => {
          YamlDataTaskToolTemplate(rw.templateType, rw.templateLocation, variablesSourceLevel).getCoreTask
        })
      ))

    })
    return list
  }

}