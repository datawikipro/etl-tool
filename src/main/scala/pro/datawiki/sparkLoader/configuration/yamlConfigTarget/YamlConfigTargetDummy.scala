package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.FileStorageTrait


@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetDummy() extends YamlConfigTargetTrait {
  @JsonIgnore
  override def writeTarget(): Boolean = {
    return true
  }
}