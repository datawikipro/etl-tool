package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*


class SparkRowElementBooleanTemplate extends SparkRowElementTypeTemplate {
  override def getType: DataType = BooleanType

  override def getValue: Any = throw Exception()
}
