package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

case class SparkRowAttributeTemplate(name: String, value: SparkRowElementTypeTemplate) {

  def getStructField: StructField = {
    return StructField(name = name, dataType = value.getType, nullable = false, metadata = Metadata.empty)
  }

  def getValueTemplate: Any = {
    return value.getValue
  }
}
