package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

trait SparkRowElementType {
  def getValue: Any

  def getType: DataType
}
