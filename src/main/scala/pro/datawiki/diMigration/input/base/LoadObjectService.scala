package pro.datawiki.diMigration.input.base


import pro.datawiki.diMigration.interfaces.FileSystemService
import pro.datawiki.diMigration.traits.LoggingTrait
import pro.datawiki.diMigration.{AttributeYamlSource, AttributeYamlTarget, MigrationProcess}


/**
 * Сервис для работы с LoadObject
 */
class LoadObjectService(fileSystemService: FileSystemService) extends LoggingTrait {

  /**
   * Выполняет миграцию для всех файлов в LoadObject
   */
  def runMigration(loadObject: LoadObject, sourceConfig: AttributeYamlSource, targetConfig: AttributeYamlTarget): Unit = {
    if (loadObject.isEmpty) {
      return
    }

    val results: List[Unit] = loadObject.getList.map(fullFileLocation => runSingleMigration(fullFileLocation, sourceConfig, targetConfig))

  }

  /**
   * Выполняет миграцию для одного файла
   */
  private def runSingleMigration(
                                  fullFileLocation: String,
                                  sourceConfig: AttributeYamlSource,
                                  targetConfig: AttributeYamlTarget
                                ): Unit = {
    val config = sourceConfig.getConfig
    MigrationProcess(config, fullFileLocation, targetConfig) match {
      case (migrationProcess) => return migrationProcess.runMigration(fullFileLocation)
      case _ => throw Exception()
    }

  }
}
