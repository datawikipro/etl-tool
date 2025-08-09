package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementBooleanTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = BooleanType

  override def getValue: Any = throw SchemaValidationException("getValue not implemented in SparkRowElementBooleanTemplate")
}
