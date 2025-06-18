package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*


class SparkRowElementBoolean(in: Boolean) extends SparkRowElementType {
  override def getValue: Any = in

  val localtype:SparkRowElementTypeTemplate = SparkRowElementBooleanTemplate()
  override def getType: DataType = localtype.getType

}
