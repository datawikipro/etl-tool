package pro.datawiki.schemaValidator.sparkRow

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementIntTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = LongType

  @JsonIgnore
  override def getValue: Any = throw SchemaValidationException("getValue not implemented in SparkRowElementIntTemplate")
}
