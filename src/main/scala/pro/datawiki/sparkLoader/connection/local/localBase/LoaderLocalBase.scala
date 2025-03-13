package pro.datawiki.sparkLoader.connection.local.localBase

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}
import java.nio.file.{Files, Paths, Path}
import java.io.File

class LoaderLocalBase(configYaml: YamlConfig) {

  def getLocation(location: String): String = {
    return s"${configYaml.folder}/${location.replace(".", "/")}"
  }

  def getLocation(location: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
    var postfix: String = ""
    keyPartitions.zipWithIndex.foreach { case (value, index) =>
      postfix = s"$postfix/${keyPartitions(index)}=${valuePartitions(index)}"
    }
    return s"${configYaml.folder}/${postfix}"
  }

  def getFolder(location: String): List[String] = {
    val path = Paths.get(location)
    if (Files.exists(path) && Files.isDirectory(path)) {
      var list:List[String] = List.apply()
      Files.list(path).filter(Files.isDirectory(_)).
        map(_.toString).toArray.toList.foreach(i=> list = list.appended(i.toString))
      return list
    } else {
      return List.empty[String]
    }
  }
}
