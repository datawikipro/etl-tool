package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

trait SparkRowElementType {
  def getValue:Any
  def getType:DataType
}
