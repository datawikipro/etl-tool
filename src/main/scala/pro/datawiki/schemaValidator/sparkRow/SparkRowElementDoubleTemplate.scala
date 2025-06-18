package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*

class SparkRowElementDoubleTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = DoubleType
  override def getValue: Any = throw Exception()
}
