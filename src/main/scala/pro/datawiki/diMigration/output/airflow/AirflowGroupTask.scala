package pro.datawiki.diMigration.output.airflow

import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class AirflowGroupTask(
                             groupTask: String,
                             airflowGroupTask: List[AirflowGroupTask],
                             airflowTask: List[AirflowTask],
                             row: Map[String, String],
                             groupTaskFileTemplate: String,
                             extraCode: String
                           ) {
  def text(inRow: Map[String, String]): String = {
    val newRow = inRow ++ row + ("group_id" -> groupTask)
    var text1 =""
    text1 +=  YamlClass.getLines(groupTaskFileTemplate, newRow)
    text1 += "\n" + airflowGroupTask.map(col => col.text(newRow)).mkString("\n")
    text1 += "\n" + airflowTask.map(col => col.text(newRow)).mkString("\n")
    text1 += "\n" + extraCode
    return text1

  }
}