package pro.datawiki.diMigration.input

import pro.datawiki.diMigration.input.base.LoadObject

import java.io.File

object LoadSas {
  def getLoadObjects(dir: String): LoadObject = {
    val loadObject: LoadObject = new LoadObject()

    val file = new File(dir)

    file.listFiles.filter(_.isFile).map(_.getPath).foreach(fullPathFile => {
      val ls = fullPathFile.split("/")
      val jobName = ls(ls.length - 1).replace(".sas", "");
      loadObject.appendJobName(jobName, fullPathFile)
    })
    return loadObject
  }


}
