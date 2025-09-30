package pro.datawiki.diMigration.test

import pro.datawiki.diMigration.interfaces.FileSystemService

/**
 * Mock реализация FileSystemService для тестирования
 */
class MockFileSystemService(
                             private val files: Map[String, List[String] ]= Map.empty,
                             private val fileContents: Map[String, String] = Map.empty,
                             private val shouldFail: Set[String] = Set.empty
                           ) extends FileSystemService {

  def listFiles(dir: String): List[String] = {
    if (shouldFail.contains(s"listFiles:$dir")) {
      throw (Exception(dir))
    } else {
      (files.getOrElse(dir, List.empty))
    }
  }

  def listFilesWithPostfix(dir: String, postfix: String): List[String] = {
    if (shouldFail.contains(s"listFilesWithPostfix:$dir")) {
      throw (Exception(dir))
    } else {
      val dirFiles = files.getOrElse(dir, List.empty)
      (dirFiles.map(_.replace(postfix, "").replace(dir, "")))
    }
  }

  def readFile(path: String):String = {
    if (shouldFail.contains(s"readFile:$path")) {
      throw (Exception(path))
    } else {
      fileContents.get(path) match {
        case Some(content) => (content)
        case None => throw Exception()
      }
    }
  }

  def writeFile(path: String, content: String): Unit = {
    if (shouldFail.contains(s"writeFile:$path")) {
      throw Exception()
    } 
  }

  def fileExists(path: String): Boolean = {
    !shouldFail.contains(s"fileExists:$path") &&
      (files.values.flatten.toSet.contains(path) || fileContents.contains(path))
  }

  def isDirectory(path: String): Boolean = {
    !shouldFail.contains(s"isDirectory:$path") && files.contains(path)
  }
}