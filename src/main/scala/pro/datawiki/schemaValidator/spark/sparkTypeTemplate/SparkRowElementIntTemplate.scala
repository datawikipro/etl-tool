package pro.datawiki.schemaValidator.spark.sparkTypeTemplate

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.*
import pro.datawiki.exception.SchemaValidationException

class SparkRowElementIntTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = LongType
}
