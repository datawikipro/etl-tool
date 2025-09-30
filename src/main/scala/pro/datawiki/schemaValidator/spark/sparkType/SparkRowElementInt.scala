package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.types.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaInt, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.SparkRowElementIntTemplate

class SparkRowElementInt(in: Long) extends SparkRowElement {
  //TODO проблема типа INT BIGINT
  override def getValue: Long = in

  override def getType: DataType = SparkRowElementIntTemplate().getType

  override def getBaseSchemaStruct: BaseSchemaStruct = BaseSchemaInt(in, false)
}
