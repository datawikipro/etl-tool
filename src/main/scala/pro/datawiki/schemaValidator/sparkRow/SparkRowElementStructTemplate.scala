package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable


class SparkRowElementStructTemplate(in: List[SparkRowAttributeTemplate]) extends SparkRowElementTypeTemplate {
  override def getType: DataType = {
    var subStruct: List[StructField] = List.apply()
    in.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })

    return StructType(subStruct.toSeq)
  }
  override def getValue: Any = throw Exception()
  def getListAttributeTemplate: List[SparkRowAttributeTemplate] = {
    return in
  }
}
