package pro.datawiki.diMigration


import pro.datawiki.diMigration.input.base.{LoadObject, LoadObjectService}
import pro.datawiki.diMigration.interfaces.{FileSystemService, ProductionFileSystemService}
import pro.datawiki.diMigration.traits.LoggingTrait

class AttributeYamlTransform(
                              source: AttributeYamlSource,
                              target: AttributeYamlTarget,
                              fileSystemService: FileSystemService = new ProductionFileSystemService()
                            ) extends LoggingTrait {
  private val loadObjectService = new LoadObjectService(fileSystemService)

  def process(): Unit = {
    val startTime = logOperationStart("transformation process")

    runMigration(source.getLoadObject)

  }

  private def runMigration(loadObject: LoadObject): Unit = {
    if (loadObject.isEmpty) {
      logInfo("No files to process after filtering")
      return 
    }

    logInfo(s"Starting migration for ${loadObject.size} files")
    loadObjectService.runMigration(loadObject, source, target)

  }
}