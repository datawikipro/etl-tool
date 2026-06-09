package pro.datawiki.schemaValidator.spark

import org.apache.spark.sql.types.{Metadata, StructField}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.SparkRowElementTypeTemplate

case class SparkRowAttributeTemplate(name: String, value: SparkRowElementTypeTemplate) {

  def getStructField: StructField = {
    return StructField(name = name, dataType = value.getType, nullable = false, metadata = Metadata.empty)
  }

}
