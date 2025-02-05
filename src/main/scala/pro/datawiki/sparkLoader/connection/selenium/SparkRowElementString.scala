package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.types.{DataType, Metadata, StringType, StructField}

class SparkRowElementString(in: String) extends SparkRowElementType{
  override def getValue:Any = in

  override def getType: DataType = StringType

}
