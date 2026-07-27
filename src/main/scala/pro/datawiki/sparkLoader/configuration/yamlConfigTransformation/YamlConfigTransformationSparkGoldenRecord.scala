package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationSparkGoldenRecord.YamlConfigTransformationSparkGoldenRecordColumnConfig
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSparkSql}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationSparkGoldenRecord(
    tableName: String,
    partitionBy: List[String],
    sourceColumn: String = "source_code",
    defaultPriority: List[String] = List("1C", "ozon", "wb", "yandex_market"),
    columns: List[YamlConfigTransformationSparkGoldenRecordColumnConfig]
) extends YamlConfigTransformationTrait {

  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    val partitionByCols = partitionBy.mkString(", ")
    val sourceColName = if (sourceColumn != null && sourceColumn.nonEmpty) sourceColumn else "source_code"

    val baseDefaultPriority = if (defaultPriority != null && defaultPriority.nonEmpty) {
      defaultPriority
    } else {
      List("1C", "ozon", "wb", "yandex_market")
    }

    val firstValueExprs = columns.map { colCfg =>
      val colName = colCfg.name
      val colPriority = if (colCfg.priority != null && colCfg.priority.nonEmpty) colCfg.priority else baseDefaultPriority
      
      val caseOrderWhen = colPriority.zipWithIndex.map { case (src, idx) =>
        s"WHEN '$src' THEN ${idx + 1}"
      }.mkString(" ")
      val caseOrderExpr = s"CASE $sourceColName $caseOrderWhen ELSE 999 END"
      s"first_value($colName, true) over (partition by $partitionByCols order by $caseOrderExpr) as $colName"
    }.mkString(",\n       ")

    val defaultCaseWhen = baseDefaultPriority.zipWithIndex.map { case (src, idx) =>
      s"WHEN '$src' THEN ${idx + 1}"
    }.mkString(" ")
    val defaultCaseExpr = s"CASE $sourceColName $defaultCaseWhen ELSE 999 END"

    val colNamesStr = columns.map(_.name).mkString(", ")

    val sqlQuery =
      s"""with ranked as (
         |select ${partitionByCols},
         |       $firstValueExprs,
         |       row_number() over (partition by $partitionByCols order by $defaultCaseExpr) as rn
         |  from $tableName)
         |select ${partitionByCols}, $colNamesStr
         |  from ranked
         | where rn = 1""".stripMargin.strip()

    TaskTemplateSparkSql(sqlQuery)
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in, false)
}
