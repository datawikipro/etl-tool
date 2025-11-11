package pro.datawiki.diMigration.input.base


import pro.datawiki.diMigration.interfaces.FileSystemService
import pro.datawiki.diMigration.traits.LoggingTrait
import pro.datawiki.diMigration.{AttributeYamlSource, AttributeYamlTarget, MigrationProcess}


class LoadObjectService(fileSystemService: FileSystemService) extends LoggingTrait {

  def runMigration(loadObject: LoadObject, sourceConfig: AttributeYamlSource, targetConfig: AttributeYamlTarget): Unit = {
    if (loadObject.isEmpty) then {
      return
    }

    val results: List[Unit] = loadObject.getList.map(fullFileLocation => runSingleMigration(fullFileLocation, sourceConfig, targetConfig))

  }

  private def runSingleMigration(
                                  fullFileLocation: String,
                                  sourceConfig: AttributeYamlSource,
                                  targetConfig: AttributeYamlTarget
                                ): Unit = {
    val config = sourceConfig.getConfig
    MigrationProcess(config, fullFileLocation, targetConfig) match {
      case migrationProcess => return migrationProcess.runMigration()
      case _ => throw Exception()
    }

  }
}
