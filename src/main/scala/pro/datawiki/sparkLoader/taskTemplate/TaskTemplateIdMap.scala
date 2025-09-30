package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode

class TaskTemplateIdMap(restore: TaskTemplateIdMapRestore,
                        in: List[TaskTemplate]) extends TaskTemplate {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    in.foreach(i => {
      val dfs = i.run(parameters = parameters, isSync = isSync)
      dfs.foreach(df => {
        LogMode.debugDF(df.getDataFrame)
      })
    })
    return restore.run(parameters = parameters, isSync = isSync)
  }

}
