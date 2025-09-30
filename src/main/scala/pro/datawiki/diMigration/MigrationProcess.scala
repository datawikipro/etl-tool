package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.InputSystem

import pro.datawiki.diMigration.input.loadYaml.YamlSourceReader
import pro.datawiki.diMigration.input.traits.SourceTrait
import pro.datawiki.diMigration.output.traits.TargetMigration
import pro.datawiki.diMigration.traits.LoggingTrait

class MigrationProcess(sourceReader: SourceTrait, targetWriter: TargetMigration) extends LoggingTrait {

  /**
   * Выполняет миграцию с proper error handling
   */
  def runMigration(fullFileLocation: String): Unit = {
    val startTime = logOperationStart("migration", s"fullFileLocation: $fullFileLocation")

    try {
      val coreDags: List[CoreDag] = sourceReader.getDagConfigs
      logInfo(s"Found ${coreDags.length} DAGs to process")

      if (coreDags.isEmpty) {
        logWarning(s"No DAGs found for fullFileLocation: $fullFileLocation")
      } else {
        val results = coreDags.map { dag =>
          try {
            targetWriter.exportDag(dag)
            (())
          } catch {
            case e: Exception =>
              logError("export DAG", e, s"fullFileLocation: $fullFileLocation")
              throw e
          }
        }
        
      }
    } catch {
      case e: Exception =>
        logError("migration", e, s"fullFileLocation: $fullFileLocation")
        throw e
    }
  }
}

object MigrationProcess {

  private def getSourceType(inSourceSystem: InputSystem, inFileName: String): SourceTrait = {
    inSourceSystem match {
      case InputSystem.Yaml =>
        try {
          (YamlSourceReader(inFileName))
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