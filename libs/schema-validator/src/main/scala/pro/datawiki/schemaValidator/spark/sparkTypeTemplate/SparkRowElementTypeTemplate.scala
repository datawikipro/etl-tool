package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

trait SparkRowElementTypeTemplate {
  def getType: DataType
}
