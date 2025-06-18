package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.{DataType, Metadata, StringType, StructField}


class SparkRowElementString(in: String) extends SparkRowElementType {
  override def getValue: Any = in

  val localtype: SparkRowElementTypeTemplate = SparkRowElementStringTemplate()

  override def getType: DataType = localtype.getType

}
