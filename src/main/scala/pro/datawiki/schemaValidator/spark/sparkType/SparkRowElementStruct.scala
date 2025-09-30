package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.SparkRowAttribute


case class SparkRowElementStruct(in: List[SparkRowAttribute]) extends SparkRowElement {
  override def getValue: Row = {
    return Row(in.map(i => i.getValue) *)
  }

  override def getType: StructType = {
    return StructType(in.map(i => i.getStructField))
  }

  def getRow: SparkRowElementRow = {
    return SparkRowElementRow(in)
  }

  override def getBaseSchemaStruct: BaseSchemaStruct = {
    return BaseSchemaObject(in.map(col => {
      (col.name, col.value.getBaseSchemaStruct)
    }), false)
  }
}
