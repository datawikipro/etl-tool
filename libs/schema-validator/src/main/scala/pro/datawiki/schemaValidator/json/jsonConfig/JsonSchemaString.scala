package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JString
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaString, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет строковое значение в JSON схеме.
 *
 * @param value Строковое значение
 */
case class JsonSchemaString(value: String) extends JsonStruct {
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaString(value, false)

  override def isEmpty: Boolean = false
}


object JsonSchemaString {
  def apply(jString: JString): JsonSchemaString = JsonSchemaString(jString.s)
}
