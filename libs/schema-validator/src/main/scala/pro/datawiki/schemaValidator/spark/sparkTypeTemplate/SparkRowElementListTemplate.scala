package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import pro.datawiki.schemaValidator.spark.SparkRowElementRowTemplate


class SparkRowElementListTemplate(baseType: SparkRowElementTypeTemplate = throw IllegalArgumentException("baseType is required")) extends SparkRowElementTypeTemplate {

  override def getType: DataType = {
    ArrayType(baseType.getType)
  }

}
