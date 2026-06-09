package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.types.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaDouble, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.SparkRowElementDoubleTemplate

class SparkRowElementDouble(in: Double) extends SparkRowElement {
  //TODO проблема типа INT BIGINT
  override def getValue: Double = in

  override def getType: DataType = SparkRowElementDoubleTemplate().getType

  override def getBaseSchemaStruct: BaseSchemaStruct = BaseSchemaDouble(in, false)
}
