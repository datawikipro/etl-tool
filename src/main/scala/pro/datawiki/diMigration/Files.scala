package pro.datawiki.diMigration



import java.io.File

object Files {

  def getListFilesInFolder(dir: String): List[String] = {
    val file = new File(dir)
    if (file.exists() && file.isDirectory) {
      file.listFiles.filter(_.isFile).map(_.getPath).toList
    } else {
      List.empty
    }
  }

  def getListFilesInFolder(dir: String, cleanPostfix: String): List[String] = {
    try {
      val file = new File(dir)
      if (file.exists() && file.isDirectory) {
        file.listFiles
          .filter(_.isFile)
          .map(_.getPath.replace(cleanPostfix, "").replace(dir, ""))
          .toList
      } else {
        List.empty
      }
    } catch {
      case _: Exception => List.empty
    }
  }

  def getListFilesInFolderSafe(dir: String): List[String] = {
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
      case e: Exception => throw (Exception("getListFilesInFolder", e))
    }
  }
}
