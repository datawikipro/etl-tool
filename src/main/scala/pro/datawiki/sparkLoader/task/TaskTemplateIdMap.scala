package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateIdMap(restore:TaskTemplateIdMapRestore,
                        in: List[TaskTemplate]) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    in.foreach(i=> {
      val dfs =  i.run(parameters = parameters, isSync = isSync)
      dfs.foreach(df=> {
        LogMode.debugDF(df.getDataFrame)
      })
    })
    return restore.run(parameters = parameters, isSync = isSync)
  }

}
