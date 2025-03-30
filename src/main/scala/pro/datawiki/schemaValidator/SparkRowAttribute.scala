package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

case class SparkRowAttribute(name:String, value:SparkRowElementType) {
  
  def getStructField:StructField ={
    return StructField(name = name, dataType = value.getType, nullable = false, metadata = Metadata.empty)
  }
  
  def getValue:Any={
    return value.getValue
  }
}
