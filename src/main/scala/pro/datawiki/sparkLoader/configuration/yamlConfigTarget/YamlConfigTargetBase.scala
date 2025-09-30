package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.context.{ApplicationContext, SparkContext}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

@JsonInclude(JsonInclude.Include.NON_ABSENT)
class YamlConfigTargetBase(connection: String,
                           mode: String = "append",
                           source: String
                          ) {
  @JsonIgnore
  def loader: ConnectionTrait = {
    ApplicationContext.getConnection(connection)
  }

  @JsonIgnore
  def loadMode: WriteMode = WriteMode(mode)

  @JsonIgnore
  def getSourceDf: DataFrameTrait = {
    val view = SparkContext.getView(source)
    if view != null then return view
    val df =SparkObject.spark.sql(s"select * from $source")
    return DataFrameOriginal(df)

  }

}