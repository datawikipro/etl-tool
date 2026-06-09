package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JDouble
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaDouble, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

case class JsonSchemaDouble(value: Double) extends JsonStruct {
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaDouble(value, false)

  override def isEmpty: Boolean = false
}

object JsonSchemaDouble {
  def apply(jDouble: JDouble): JsonSchemaDouble = JsonSchemaDouble(jDouble.num)
}
