package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.JsonInclude
import com.google.cloud.spark.bigquery.repackaged.io.openlineage.spark.shade.com.fasterxml.jackson.annotation.JsonIgnore

@JsonInclude(JsonInclude.Include.NON_ABSENT)
trait YamlConfigTargetTrait {
  @JsonIgnore
  def writeTarget(): Boolean
}
