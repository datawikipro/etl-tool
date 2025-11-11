package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreControlDag, CoreDag}
import pro.datawiki.diMigration.core.dictionary.InputSystem
import pro.datawiki.diMigration.input.loadYaml.YamlSourceReader
import pro.datawiki.diMigration.input.traits.SourceTrait
import pro.datawiki.diMigration.output.airflow.AirflowPipelineStart
import pro.datawiki.diMigration.output.traits.{TargetDag, TargetMigration}
import pro.datawiki.diMigration.traits.LoggingTrait

class MigrationProcess(sourceReader: SourceTrait, targetWriter: TargetMigration) extends LoggingTrait {

  def runMigration(): Unit = {
    val startTime = logOperationStart("migration", s"fullFileLocation: ${sourceReader.getControlName}")

    val coreDags: List[CoreDag] = sourceReader.getDagConfigs
    logInfo(s"Found ${coreDags.length} DAGs to process")

    if (coreDags.isEmpty) {
      logWarning(s"No DAGs found for fullFileLocation: ${sourceReader.getControlName}")
      return
    }
    val results: List[TargetDag] = coreDags.map { dag =>
      try {
        targetWriter.exportDag(dag)
      } catch {
        case e: Exception =>
          logError("export DAG", e, s"fullFileLocation: ${sourceReader.getControlName}")
          throw e
      }
    }

    if results.nonEmpty then {
      val controlDag =
        targetWriter.exportDag(
          CoreControlDag(
            dagName = sourceReader.getControlName,
            tags = List.apply("control"),
            dagDescription = "control dag",
            schedule = sourceReader.getSchedule,
            retries = 0,
            startDate = null,
            catchup = false,
            pythonFile = s"/opt/etl-tool/configMigrationOutput/dags/${sourceReader.getControlName}.py", //TODO
            taskPipelines = results.map(col => {
              (col.getTaskPipelines.map(col2 => AirflowPipelineStart(dagName = col.getDagName, taskName = col2.getTaskName)), false)
            })
          )
        )
    }
  }

}


object MigrationProcess {

  private def getSourceType(inSourceSystem: InputSystem, inFileName: String): SourceTrait = {
    inSourceSystem match {
      case InputSystem.Yaml =>
        try {
          YamlSourceReader(inFileName)
        } catch {
          case e: Exception => throw e
        }
      case _ => throw (Exception(inSourceSystem.toString))
    }
  }

  private def getTargetType(in: AttributeYamlTarget): TargetMigration = {
    TargetMigration(in.getConfig, in.getLocation, in.getTemplateLocation)
  }

  def apply(inSourceSystem: InputSystem, inFileName: String, inTarget: AttributeYamlTarget): MigrationProcess = {

    val sourceReader = getSourceType(inSourceSystem, inFileName)
    val targetWriter = getTargetType(inTarget)
    new MigrationProcess(sourceReader, targetWriter)
  }
}