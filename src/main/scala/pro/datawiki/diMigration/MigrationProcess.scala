package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.InputSystem
import pro.datawiki.diMigration.input.loadYaml.YamlSourceReader
import pro.datawiki.diMigration.input.traits.SourceTrait
import pro.datawiki.diMigration.output.traits.TargetMigration

class MigrationProcess(sourceReader: SourceTrait, targetWriter: TargetMigration) {

  def runMigration(jobName: String): Unit = {
    val coreDags: List[CoreDag] = sourceReader.getDagConfigs
    coreDags.foreach(targetWriter.exportDag)
  }
}


object MigrationProcess {

  private def getSourceType(inSourceSystem: InputSystem, inFileName: String): SourceTrait = {
    inSourceSystem match
      case InputSystem.Yaml => return YamlSourceReader(inFileName)
      case _ => throw Exception()

  }

  private def getTargetType(in: AttributeYamlTarget): TargetMigration = {
    return TargetMigration(in.getConfig, in.getLocation, in.getTemplateLocation)
  }

  def apply(inSourceSystem: InputSystem, inFileName: String, inTarget: AttributeYamlTarget): MigrationProcess = {
    return new MigrationProcess(getSourceType(inSourceSystem, inFileName), getTargetType(inTarget))
  }
}