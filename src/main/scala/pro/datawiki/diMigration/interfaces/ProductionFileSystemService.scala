package pro.datawiki.diMigration.interfaces



/**
 * Production реализация FileSystemService
 */
class ProductionFileSystemService extends FileSystemService {

  import java.io.File
  import java.nio.file.{Files, Paths}

  def listFiles(dir: String): List[String] = {
    try {
      val file = new File(dir)
      if (!file.exists()) {
        throw (Exception(dir))
      } else if (!file.isDirectory) {
        throw (Exception(s"Path $dir is not a directory"))
      } else {
        val files = file.listFiles.filter(_.isFile).map(_.getPath).toList
        (files)
      }
    } catch {
      case e: Exception => throw e
    }
  }

  def listFilesWithPostfix(dir: String, postfix: String): List[String] = {
    try {
      val file = new File(dir)
      if (!file.exists()) {
        throw (Exception(dir))
      } else if (!file.isDirectory) {
        throw (Exception(s"Path $dir is not a directory"))
      } else {
        val files = file.listFiles
          .filter(_.isFile)
          .map(_.getPath.replace(postfix, "").replace(dir, ""))
          .toList
        (files)
      }
    } catch {
      case e: Exception =>throw e
    }
  }

  def readFile(path: String):String = {
    try {
      val content = Files.readString(Paths.get(path))
      (content)
    } catch {
      case e: Exception => throw e
    }
  }

  def writeFile(path: String, content: String): Unit = {
    try {
      Files.write(Paths.get(path), content.getBytes)
      (())
    } catch {
      case e: Exception =>throw e
    }
  }

  def fileExists(path: String): Boolean = {
    new File(path).exists()
  }

  def isDirectory(path: String): Boolean = {
    new File(path).isDirectory
  }
}
