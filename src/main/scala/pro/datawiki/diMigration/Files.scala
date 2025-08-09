package pro.datawiki.diMigration

import java.io.File

object Files {
  def getListFilesInFolder(dir: String): List[String] = {
    val file = new File(dir)
    val list = file.listFiles.filter(_.isFile).map(_.getPath).toList
    return list
  }

  def getListFilesInFolder(dir: String, cleanPostfix: String): List[String] = {
    try
      val file = new File(dir)
      val list = file.listFiles.filter(_.isFile).map(_.getPath.replace(cleanPostfix, "").replace(dir, "")).toList
      return list
    catch
      case _ => return List.apply()
  }

}
