package pro.datawiki.diMigration.input.base

import pro.datawiki.diMigration.{AttributeYamlSource, AttributeYamlTarget, MigrationProcess}

class LoadObject {
  private var processFilesMap: Map[String, String] = Map()
  private var processFilesList: List[String] = List.apply()

  def appendJobName(jobName: String, fullPathFile: String): Unit = {
    processFilesMap += (jobName -> fullPathFile)
    processFilesList = processFilesList.appended(jobName)
  }

  def intersect(list: List[String]): Unit = {
    processFilesList = processFilesList.intersect(list)
  }

  def diff(list: List[String]): Unit = {
    processFilesList = processFilesList.diff(list)
  }

  def getList: List[(String, String)] = {
    var result: List[(String, String)] = List.apply()
    processFilesList.foreach(i => {
      result = result.appended(i, processFilesMap(i))
    })

    return result
  }

  private def runRow(jobName: String, fullFileLocation: String, sourceConfig: AttributeYamlSource, targetConfig: AttributeYamlTarget): Unit = {
    val migrationProcess: MigrationProcess = MigrationProcess(sourceConfig.getConfig, fullFileLocation,targetConfig)
    migrationProcess.runMigration(jobName)
  }

  def run(sourceConfig: AttributeYamlSource, targetConfig: AttributeYamlTarget): Boolean = {
    getList.foreach(i => {
      runRow(jobName = i._1, fullFileLocation = i._2, sourceConfig, targetConfig)
    })
    return true
  }

}
