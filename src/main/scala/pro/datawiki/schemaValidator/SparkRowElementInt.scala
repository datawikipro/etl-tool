package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.{DataType, Metadata, StringType, StructField, IntegerType}

class SparkRowElementInt(in: BigInt) extends SparkRowElementType {
  //TODO проблема типа INT BIGINT
  override def getValue: Any = in.toInt

  override def getType: DataType = IntegerType
}
