package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementDoubleTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = DoubleType
}
