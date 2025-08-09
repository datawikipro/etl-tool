package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, WriteMode}
import pro.datawiki.sparkLoader.task.{Context, Task}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
class YamlConfigTargetBase(connection: String,
                           mode: String = "append",
                           partitionMode: String,
                           source: String
                          ) {
  @JsonIgnore
  def loader: ConnectionTrait = {
    Context.getConnection(connection)
  }
  @JsonIgnore
  def loadMode: WriteMode = WriteMode(mode)
  @JsonIgnore
  def getSourceDf: DataFrameTrait = {
    val view  = Task.getView(source)
    if view == null then 
      return DataFrameOriginal(SparkObject.spark.sql(s"select * from $source"))
    return view 
  }

}