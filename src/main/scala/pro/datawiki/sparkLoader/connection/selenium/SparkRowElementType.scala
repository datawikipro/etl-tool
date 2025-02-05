package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

trait SparkRowElementType {
  def test2:String = {throw Exception()}
  def getValue:Any
  def getType:DataType
}
