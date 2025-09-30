package pro.datawiki.diMigration.input

import pro.datawiki.diMigration.input.base.LoadObject

import java.io.File

object LoadSas {
  def getLoadObjects(dir: String): LoadObject = {
    val file = new File(dir)

    return new LoadObject(file.listFiles.filter(_.isFile).map(_.getPath).toList)
  }


}
