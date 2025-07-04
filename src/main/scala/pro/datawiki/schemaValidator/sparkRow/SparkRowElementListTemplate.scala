package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}


class SparkRowElementListTemplate(in: Seq[SparkRowElementTypeTemplate]) extends SparkRowElementTypeTemplate {

  override def getType: DataType = {
    if in.isEmpty then return ArrayType(StructType(List.apply()))
    in.head match {
      case x: SparkRowElementRowTemplate => {

        val a: SparkRowElementRowTemplate = x
        var subStruct: List[StructField] = List.apply()
        a.getSparkRowListAttributeTemplate.foreach(i => {
          subStruct = subStruct :+ i.getStructField
        })

        return ArrayType(StructType(subStruct))
      }
      case x: SparkRowElementStructTemplate => {
        
        var subStruct: List[StructField] = List.apply()
        x.getListAttributeTemplate.foreach(i => {
          subStruct = subStruct :+ i.getStructField
        })

        return ArrayType(StructType(subStruct))
      }
      case x: SparkRowElementStringTemplate => {
        return ArrayType(x.getType)
      }
      case _ => {
        throw Exception()
      }
    }
  }

  override def getValue: Any = throw Exception()
}
