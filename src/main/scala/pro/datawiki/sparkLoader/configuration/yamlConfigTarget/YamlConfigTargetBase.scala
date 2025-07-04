package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, WriteMode}
import pro.datawiki.sparkLoader.task.{Context, Task}

class YamlConfigTargetBase(connection: String,
                           mode: String = "append",
                           partitionMode: String,
                           source: String
                          ) {
  def loader: ConnectionTrait = {
    Context.getConnection(connection)
  }

  def loadMode: WriteMode = {
    mode match
      case "overwrite" => WriteMode.overwrite
      case "append" => WriteMode.append
      case "stream" => WriteMode.append
      case "merge" => WriteMode.merge
      case "streamByRunId" => WriteMode.append
      case _ => {
        throw Exception()
      }
  }

  def getSourceDf: DataFrameTrait = {
    val view  = Task.getView(source)
    if view == null then 
      return DataFrameOriginal(SparkObject.spark.sql(s"select * from $source"))
    return view 
  }

}