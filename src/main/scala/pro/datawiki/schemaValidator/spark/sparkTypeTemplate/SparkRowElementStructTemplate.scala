package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import pro.datawiki.schemaValidator.spark.SparkRowAttributeTemplate

import scala.collection.mutable


case class SparkRowElementStructTemplate(in: List[SparkRowAttributeTemplate]) extends SparkRowElementTypeTemplate {
  override def getType: DataType = {
    var subStruct: List[StructField] = List.apply()
    in.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })

    return StructType(subStruct.toSeq)
  }

  def getListAttributeTemplate: List[SparkRowAttributeTemplate] = {
    return in
  }
}
