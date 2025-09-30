package pro.datawiki.sparkLoader.connection.local.localBase

import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

import java.io.IOException
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

class LoaderLocalBase(configYaml: YamlConfig) {

  def saveRaw(in: String, location: String): Unit = {
    reflect.io.File(getLocation(location)).writeAll(in)
  }

  def getLocation(location: String): String = {
    return s"${configYaml.folder}/${location}"
  }

  def getLocation(location: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
    var postfix: String = ""
    keyPartitions.zipWithIndex.foreach { case (value, index) =>
      postfix = s"$postfix/${keyPartitions(index)}=${valuePartitions(index)}"
    }
    val tgt = s"${configYaml.folder}/$location/${postfix}" 
    return tgt
  }

  def getFolder(location: String): List[String] = {
    val path = Paths.get(s"${configYaml.folder}/$location/")
    if (Files.exists(path) && Files.isDirectory(path)) {
      val a = Files.list(path).filter(Files.isDirectory(_)).toArray()
      if a.nonEmpty then {
        var list: List[String] = List.apply()
        a.map(_.toString).toArray.toList.foreach(i => {
          list = list.appended(i.toString)
        })
        return list
      } else {
        return List.apply("")
      }


    } else {
      return List.empty[String]
    }
  }

  def getMasterFolder: String = configYaml.folder

  def moveTablePartition(sourceSchema: String, oldTable: String, newTable: String, partitionName: List[String]): Boolean = {
    val sourcePath = Paths.get(s"${sourceSchema}/$oldTable")
    val targetPath = Paths.get(s"${sourceSchema}/$newTable")

    // Проверяем существование исходной директории
    if (!Files.exists(sourcePath) || !Files.isDirectory(sourcePath)) {
      throw new IllegalArgumentException(s"Source directory does not exist: $sourcePath")
    }

    // Создаем целевую директорию если нужно
    if (!Files.exists(targetPath)) {
      Files.createDirectories(targetPath)
    } else if (!Files.isDirectory(targetPath)) {
      throw new IllegalArgumentException(s"Target path is not a directory: $targetPath")
    }

    // Получаем список файлов (игнорируя поддиректории)
    val fileList = Files.list(sourcePath)
      .filter(Files.isRegularFile(_))
      .iterator()

    // Перемещаем файлы
    while (fileList.hasNext) {
      val sourceFile = fileList.next()
      val targetFile = targetPath.resolve(sourceFile.getFileName)

      try {
        Files.move(
          sourceFile,
          targetFile,
          StandardCopyOption.REPLACE_EXISTING
        )
        //        println(s"Moved: ${sourceFile.getFileName} -> $targetPath")
      } catch {
        case e: IOException =>
          println(s"Failed to move ${sourceFile.getFileName}: ${e.getMessage}")
      }
    }
    return true
  }
}
