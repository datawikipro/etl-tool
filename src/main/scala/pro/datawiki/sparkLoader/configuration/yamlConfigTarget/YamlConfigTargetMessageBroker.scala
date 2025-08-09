package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigTargetTrait}
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, QueryTrait}
import pro.datawiki.sparkLoader.task.Context

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetMessageBroker(
                                       connection: String,
                                       source: String,
                                       mode: String = "append",
                                       target: String,
                                       partitionMode:String
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, partitionMode = partitionMode, source = source), YamlConfigTargetTrait {
  @JsonIgnore
  override def loader: QueryTrait = {
    super.loader match
      case x: QueryTrait => x
      case _ => throw Exception()
  }
  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf
    loader.createTopic(target)
    throw Exception()
  }
}