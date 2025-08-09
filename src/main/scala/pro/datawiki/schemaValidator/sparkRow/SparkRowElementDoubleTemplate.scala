package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementDoubleTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = DoubleType
  override def getValue: Any = throw SchemaValidationException("getValue not implemented in SparkRowElementDoubleTemplate")
}
