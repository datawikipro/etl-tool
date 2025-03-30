package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.{BooleanType, DataType, Metadata, StringType, StructField}


class SparkRowElementBoolean(in: Boolean) extends SparkRowElementType{
  override def getValue:Any = in

  override def getType: DataType = BooleanType

}
