package pro.datawiki.schemaValidator.spark

import org.apache.spark.sql.types.{Metadata, StructField}
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement

case class SparkRowAttribute(name: String, value: SparkRowElement) {

  def getStructField: StructField = {
    return StructField(name = name, dataType = value.getType, nullable = false, metadata = Metadata.empty)
  }

  def getValue: Any = {
    return value.getValue
  }


}
