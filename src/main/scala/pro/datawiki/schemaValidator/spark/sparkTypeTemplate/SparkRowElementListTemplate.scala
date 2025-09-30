package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import pro.datawiki.schemaValidator.spark.SparkRowElementRowTemplate


class SparkRowElementListTemplate(baseType: SparkRowElementTypeTemplate = throw IllegalArgumentException("baseType is required")) extends SparkRowElementTypeTemplate {

  override def getType: DataType = {
    baseType match {
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
        throw UnsupportedOperationException("Unsupported list element type")
      }
    }
  }

}
