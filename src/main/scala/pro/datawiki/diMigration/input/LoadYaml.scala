package pro.datawiki.diMigration.input

import pro.datawiki.diMigration.input.base.LoadObject

import java.io.File

object LoadYaml {

  private def getDagTemplatesFolder(dir: String): Array[String] = {
    val file = new File(dir)
    if (!file.exists() || !file.isDirectory) {
      return Array.empty[String]
    }

    val result = scala.collection.mutable.ArrayBuffer[String]()

    def findTemplateFiles(currentDir: File): Unit = {
      val files = currentDir.listFiles()
      if (files != null) {
        val directories = files.filter(_.isDirectory)
        val fileFiles = files.filter(_.isFile)

        // Check each file - if it's not a YAML file, throw an error
        fileFiles.foreach(file => {
          if (!file.getName.endsWith(".yaml")) {
            throw new Exception(s"Non-YAML file found in template directory: ${file.getPath}")
          }
          result += file.getPath
        })

        // Recursively search subdirectories
        directories.foreach(findTemplateFiles)
      }
    }

    findTemplateFiles(file)
    return result.toArray
  }

  private def checkTemplate(dir: String): (String, String) = {
    val files = new File(dir).listFiles.filter(_.isFile)
    val folders = new File(dir).listFiles.filter(_.isDirectory)
    if files.length != 1 then {
      throw Exception("Wrong Yaml template")
    }

    val fileFullPath: String = files.head.getPath

    val fileName = files.head.getName
    if fileName.split(".".toCharArray.head).last != "yaml" then {
      throw Exception("Wrong Yaml template")
    }

    val jobName = fileName.substring(0, fileName.length - 5)
    return (jobName, fileFullPath)
  }

  def getLoadObjects(dir: String): LoadObject = {
    return LoadObject(getDagTemplatesFolder(dir = dir).toList)
  }
}
