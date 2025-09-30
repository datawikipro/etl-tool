package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.types.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBoolean, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.SparkRowElementBooleanTemplate


class SparkRowElementBoolean(in: Boolean) extends SparkRowElement {
  override def getValue: Any = in

  override def getType: DataType = SparkRowElementBooleanTemplate().getType

  override def getBaseSchemaStruct: BaseSchemaStruct = BaseSchemaBoolean(in, false)
}
