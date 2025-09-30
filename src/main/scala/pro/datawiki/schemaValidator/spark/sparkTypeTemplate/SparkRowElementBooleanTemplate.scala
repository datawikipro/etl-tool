package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementBooleanTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = BooleanType

}
