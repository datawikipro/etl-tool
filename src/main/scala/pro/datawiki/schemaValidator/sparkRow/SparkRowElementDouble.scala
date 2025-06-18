package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*

class SparkRowElementDouble(in: Double) extends SparkRowElementType {
  //TODO проблема типа INT BIGINT
  override def getValue: Any = in.toDouble

  val localtype:SparkRowElementTypeTemplate = SparkRowElementDoubleTemplate()
  override def getType: DataType = localtype.getType
}
