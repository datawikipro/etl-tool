package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JInt
import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaInt, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

/**
 * Представляет целочисленное значение в JSON схеме.
 *
 * @param value Целочисленное значение
 */
case class JsonSchemaIntTemplate() extends JsonSchemaTemplate {
  override def generateJValue(): JValue = {
    JObject("type" -> JString("integer"))
  }
}
