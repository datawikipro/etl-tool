package pro.datawiki.schemaValidator.sparkRow

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.{DataType, Metadata, StringType, StructField}
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementStringTemplate extends SparkRowElementTypeTemplate {
  
  override def getType: DataType = StringType

  @JsonIgnore
  override def getValue: Any = throw SchemaValidationException("getValue not implemented in SparkRowElementStringTemplate")
  
}
