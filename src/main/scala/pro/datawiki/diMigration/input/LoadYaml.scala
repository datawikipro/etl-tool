package pro.datawiki.diMigration.input

import pro.datawiki.diMigration.input.base.LoadObject

import java.io.File

object LoadYaml {

  private def getDagTemplatesFolder(dir: String): Array[String] = {
    val file = new File(dir)
    val result: Array[String] = file.listFiles.filter(_.isDirectory).map(_.getPath)
    return result
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
    val loadObject: LoadObject = new LoadObject()

    getDagTemplatesFolder(dir = dir).foreach(fullPathFile => {
      val v1 = checkTemplate(fullPathFile)
      loadObject.appendJobName(v1._1, v1._2)
    })
    return loadObject
  }
}
