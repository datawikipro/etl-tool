package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.context.SparkContext

/**
 * Configuration for stdout target.
 * Outputs DataFrame to stdout in specified format.
 * Used for integration scenarios where ETL results need to be captured programmatically.
 *
 * @param source DataFrame name from transformations
 * @param format Output format: json, json-array, jsonl, csv
 */
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetStdout(
                                   source: String,
                                   format: String = "json"
                                 ) extends YamlConfigTargetTrait {

  // Markers for reliable parsing in silent mode
  private val START_MARKER = "---ETL_OUTPUT_START---"
  private val END_MARKER = "---ETL_OUTPUT_END---"

  @JsonIgnore
  private def getSourceDf: DataFrameTrait = {
    val view = SparkContext.getView(source)
    if view != null then return view
    val df = SparkObject.spark.sql(s"select * from $source")
    return DataFrameOriginal(df)
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df = getSourceDf.getDataFrame

    // Print markers only when NOT in debug mode (for clean parsing)
    if (!LogMode.isDebug) {
      System.out.println(START_MARKER)
    }

    format.toLowerCase match {
      case "json" | "json-array" =>
        // Collect as JSON array
        val jsonArray = df.toJSON.collect().mkString("[", ",", "]")
        System.out.println(jsonArray)

      case "jsonl" =>
        // JSON Lines format (one JSON per line)
        df.toJSON.collect().foreach(System.out.println)

      case "csv" =>
        // CSV format with header
        System.out.println(df.columns.mkString(","))
        df.collect().foreach(row =>
          System.out.println(row.toSeq.map(v => if v == null then "" else v.toString).mkString(","))
        )

      case _ =>
        // Fallback: JSON array
        val jsonArray = df.toJSON.collect().mkString("[", ",", "]")
        System.out.println(jsonArray)
    }

    if (!LogMode.isDebug) {
      System.out.println(END_MARKER)
    }

    return true
  }
}
