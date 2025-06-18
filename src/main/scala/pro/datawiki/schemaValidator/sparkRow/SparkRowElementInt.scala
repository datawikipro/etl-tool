package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.*

class SparkRowElementInt(in: BigInt) extends SparkRowElementType {
  //TODO проблема типа INT BIGINT
  override def getValue: Any = in.toLong

  val localtype:SparkRowElementTypeTemplate = SparkRowElementIntTemplate()
  override def getType: DataType = localtype.getType
}
