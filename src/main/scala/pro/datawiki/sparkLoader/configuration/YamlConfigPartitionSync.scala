package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigPartitionSync(
                                     enabled: Boolean = true,
                                     partitionColumn: String = "partition",
                                     lookbackDays: Option[Int] = None,
                                     partitionList: List[String] = List.empty,
                                     metrics: List[String] = List("count"),
                                     hashExpression: Option[String] = None,
                                     odsConnection: String,
                                     odsConfigLocation: Option[String] = None,
                                     odsTable: String,
                                     scdType: String = "SCD_1",
                                     scdActiveFilter: Option[String] = None
                                   )
