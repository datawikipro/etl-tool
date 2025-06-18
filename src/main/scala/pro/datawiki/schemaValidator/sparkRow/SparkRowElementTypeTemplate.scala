package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

trait SparkRowElementTypeTemplate {
  def getType: DataType
  def getValue: Any
}
