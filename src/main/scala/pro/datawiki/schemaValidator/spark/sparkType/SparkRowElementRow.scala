package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.SparkRowAttribute

case class SparkRowElementRow(in: List[SparkRowAttribute]) extends SparkRowElement {

  def getRow: Row = {
    var lst2: List[Any] = List.apply()
    in.foreach(i => lst2 = lst2 :+ i.getValue)
    val tst = Row.apply(lst2 *)
    return tst
  }

  def getBaseSchemaObject: BaseSchemaObject = {
    return BaseSchemaObject(in.map(i => (i.name, i.value.getBaseSchemaStruct)), false)
  }

  override def getValue: Any  = {
    return Row(in.map(i => i.getValue) *)
  }

  override def getType: DataType = throw NotImplementedException("getType not implemented for SparkRowElementRow")

  override def getBaseSchemaStruct: BaseSchemaStruct = {
    return getBaseSchemaObject
  }
}

object SparkRowElementRow {
  def apply(in: List[SparkRowAttribute]): SparkRowElementRow = new SparkRowElementRow(in)

  def apply(in: SparkRowAttribute): SparkRowElementRow = new SparkRowElementRow(List(in))

  def apply(row: Row, schema: StructType): SparkRowElementRow = {
    return SparkRowElementRow(schema.fields.map(i => SparkRowAttribute(i.name, SparkRowElement(i.dataType, row.getAs(i.name)))).toList)
  }

}
