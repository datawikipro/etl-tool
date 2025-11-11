package pro.datawiki.diMigration.output.airflow

import pro.datawiki.diMigration.output.traits.{TargetDag, TargetTask}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class AirflowTask(
                       taskName: String,
                       taskFileTemplate: String,
                       row: Map[String, String],
                       extraCode: String
                     ) extends TargetTask{
  def text(inRow: Map[String, String]): String = {
    var text1 = ""
    text1 += YamlClass.getLines(taskFileTemplate, inRow ++ row + ("taskName" -> taskName))
    if extraCode != "" then text1 += "\n"+ extraCode
    return text1
  }

  override def getTaskName: String = taskName
}
