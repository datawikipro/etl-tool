package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JInt
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaInt, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет целочисленное значение в JSON схеме.
 *
 * @param value Целочисленное значение
 */
case class JsonSchemaInt(value: Long) extends JsonStruct {
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaInt(value, false)

  override def isEmpty: Boolean = false
}

object JsonSchemaInt {
  def apply(jInt: JInt): JsonSchemaInt = JsonSchemaInt(jInt.num.toLong)
}
