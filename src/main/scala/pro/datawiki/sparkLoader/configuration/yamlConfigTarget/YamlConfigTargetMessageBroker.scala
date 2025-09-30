package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.configuration.{ YamlConfigTargetTrait}
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, QueryTrait}


@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetMessageBroker(
                                          connection: String,
                                          source: String,
                                          mode: String = "append",
                                          target: String,
                                          partitionMode: String
                                        ) extends YamlConfigTargetBase(connection = connection, mode = mode, source = source), YamlConfigTargetTrait {
  @JsonIgnore
  override def loader: QueryTrait = {
    super.loader match
      case x: QueryTrait => x
      case _ => throw IllegalArgumentException("Invalid loader type for message broker")
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf
    loader.createTopic(target)
    throw NotImplementedException("Message broker writing functionality not implemented")
  }
}