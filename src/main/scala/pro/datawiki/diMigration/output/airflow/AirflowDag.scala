package pro.datawiki.diMigration.output.airflow

import pro.datawiki.diMigration.output.traits.{TargetDag, TargetTask}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class AirflowDag(
                       dagName: String,
                       pipelineStart: List[AirflowPipelineStart],
                       dagFileTemplate: String,
                       targetPythonFile: String,
                       row: Map[String, String],
                       airflowGroup: List[AirflowGroupTask],
                       tasks: List[AirflowTask],
                       extraCode: String
                     ) extends TargetDag {

  var text = YamlClass.getLines(dagFileTemplate, row)
  text += "\n" + airflowGroup.map(col => col.text(row)).mkString("\n")
  text += "\n" + tasks.map(col => col.text(row)).mkString("\n")
  text += "\n" + extraCode
  YamlClass.writefile(targetPythonFile, text)

  override def getTaskPipelines: List[TargetTask] = {
    pipelineStart.map(
      col =>AirflowTask(
        taskName = col.taskName,
        taskFileTemplate = "taskTemplateTriggerDagRunOperator.py",
        row = row + ("dagName" -> col.dagName)+ ("defaultPipe" -> col.taskName),
        extraCode = "") )
  }

  override def getDagName: String = dagName
}
