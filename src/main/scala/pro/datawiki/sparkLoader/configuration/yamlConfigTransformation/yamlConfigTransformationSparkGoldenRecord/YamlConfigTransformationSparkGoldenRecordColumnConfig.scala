package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationSparkGoldenRecord

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationSparkGoldenRecordColumnConfig(
    name: String,
    priority: List[String] = Nil
)
