package pro.datawiki.diMigration.output.etlTool

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreControlDag, CoreDag}
import pro.datawiki.diMigration.core.task.CoreTaskEtlToolTemplate
import pro.datawiki.diMigration.output.airflow.{AirflowDag, AirflowGroupTask, AirflowPipelineStart, AirflowTask}
import pro.datawiki.diMigration.output.traits.{TargetDag, TargetMigration}
import pro.datawiki.sparkLoader.configuration.*
import pro.datawiki.yamlConfiguration.YamlClass

import java.text.SimpleDateFormat
import scala.collection.mutable

class EtlToolMigration(targetLocation: String, templateLocation: String) extends TargetMigration {

  private def exportYamlEtlTool(inCoreTask: CoreTaskEtlToolTemplate, row: Map[String, String]): Unit = {
    val exp: EltConfig = new EltConfig(
      connections = inCoreTask.connections.map(con => con.getEtToolFormat),
      preEtlOperations = inCoreTask.preEtlOperations.map(con => con.getEtToolFormat),
      source = inCoreTask.sources.map(con => con.getEtToolFormat),
      transformations = inCoreTask.transform.map(con => con.getEtToolFormat),
      target = inCoreTask.target.map(con => con.getEtToolFormat),
      postEtlOperations = inCoreTask.postEtlOperations.map(con => con.getEtToolFormat),
    )

    val yamlText = YamlClass.toYaml(exp)
    YamlClass.writefile(s"${inCoreTask.yamlFile}", yamlText)
  }

  override def exportDag(in: CoreDag): TargetDag = {
    in match {
      case x: CoreBaseDag => {
        val row: Map[String, String] = Map.apply(
          ("dagName" -> x.dagName),
          ("dagDescription" -> x.dagDescription),
          ("catchup" -> (x.catchup match {
            case true => "True"
            case false => "False"
          })),
          ("retries" -> x.retries.toString),
          ("start_date" -> (x.startDate match {
            case null => "days_ago(0)"
            case date => {
              val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
              s"datetime(${date.getYear + 1900}, ${date.getMonth + 1}, ${date.getDate})"
            }
          })),
          ("schedule" -> x.schedule.getAirflowFormat),
          ("tags" -> x.tags.map(col => s"'${col}'").mkString(",")),
          ("defaultPipe" -> x.taskPipelines.head._1.head.getTaskId),
        )

        val airflowGroupTask: List[AirflowGroupTask] = x.taskPipelines.map(y => {
          AirflowGroupTask(
            groupTask = y._1.head.getTaskId,
            airflowGroupTask = List.empty,
            airflowTask = y._1.map {
              case x: CoreTaskEtlToolTemplate => {
                exportYamlEtlTool(x, row)
                AirflowTask(
                  taskName = x.taskName,
                  taskFileTemplate = s"$getTargetLocation/taskTemplateEtlToolOperator.py",
                  row = Map(
                    ("yamlFile" -> x.yamlFile),
                    ("dependencies" -> s"[${x.dependencies.mkString(", ")}] >> ${x.taskName}")),
                  extraCode = ""
                )
              }
              case x: AirflowPipelineStart => {
                AirflowTask(
                  taskName = x.taskName,
                  taskFileTemplate = s"$getTargetLocation/taskTemplateTriggerDagRunOperator.py",
                  row = Map(
                    ("dagName" -> x.dagName),
                    ("taskName" -> x.taskName),
                    ("dependencies" -> s"")
                  ),
                  extraCode = ""
                )
              }
            },
            row = Map(),
            groupTaskFileTemplate = s"$getTargetLocation/taskTemplateGroupOperator.py",
            extraCode = s"\n    branching >> ${y._1.head.getTaskId}"
          )


        })

        return AirflowDag(
          dagName = x.dagName,
          pipelineStart = {
            x.taskPipelines.map(y => {
              y._2 match {
                case false => null
                case _ => AirflowPipelineStart(x.dagName, y._1.head.getTaskId)
              }
            }).filterNot(col => col == null)
          },
          dagFileTemplate = s"$getTargetLocation/dagTemplate.py",
          targetPythonFile = x.pythonFile,
          row = row,
          airflowGroup = airflowGroupTask,
          tasks = List.empty,
          extraCode = ""
        )

      }
      case x: CoreControlDag => {
        val row: Map[String, String] = Map(
          ("dagName" -> x.dagName),
          ("dagDescription" -> x.dagDescription),
          ("catchup" -> (x.catchup match {
            case true => "True"
            case false => "False"
          })),
          ("start_date" -> (x.startDate match {
            case null => "days_ago(0)"
            case date => {
              val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
              s"datetime(${date.getYear + 1900}, ${date.getMonth + 1}, ${date.getDate})"
            }
          })),
          ("schedule" -> x.schedule.getAirflowFormat),
          ("tags" -> x.tags.map(col => s"'${col}'").mkString(",")),
          ("defaultPipe" -> ""),
        )

        val airflowTask: List[AirflowTask] = x.taskPipelines.flatMap(y => {
          y._1.map {
            case x1: AirflowPipelineStart => {
              AirflowTask(
                taskName = x1.dagName,
                taskFileTemplate = s"$getTargetLocation/taskTemplateTriggerDagRunOperator.py",
                row = Map(
                  ("dagName" -> x1.dagName),
                  ("defaultPipe" -> x1.taskName),
                  ("dependencies" -> s"")
                ),
                extraCode = ""
              )
            }
            case _ => throw Exception()
          }
        }
        )

        return AirflowDag(
          dagName = x.dagName,
          pipelineStart = List.empty,
          dagFileTemplate = s"$getTargetLocation/dagTemplate2.py",
          targetPythonFile = x.pythonFile,
          row = row,
          airflowGroup = List.empty,
          tasks = airflowTask,
          extraCode = ""
        )

      }
      case _ => {
        throw UnsupportedOperationException("Unsupported migration case")
      }
    }
  }

  override def getTargetLocation: String = templateLocation
}
