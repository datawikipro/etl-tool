package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.WriteMode
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import scala.collection.mutable

class TaskSimple(inTaskTemplate: TaskTemplate) extends Task {
  var cache: TransformationCacheTrait = null

  override def run(targetName: String, parameters: mutable.Map[String, String], isSync:Boolean): Boolean = {
    var df: List[DataFrameTrait] = inTaskTemplate.run(parameters = parameters, true)
    if cache != null then {
      if df.length != 1 then throw Exception()
      df.head match
        case x: DataFrameOriginal => {
          cache.saveTable(x, WriteMode.overwrite)
          return Task.saveDf(targetName, cache.readDirty())
        }
        case _ => throw Exception()
    }

    Task.saveDf(targetName, df)
  }

  override def setCache(in: TransformationCacheTrait): Unit = {
    cache = in
  }
}
