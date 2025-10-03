package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JString
import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaString, BaseSchemaStringTemplate, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

/**
 * Представляет строковое значение в JSON схеме.
 *
 * @param value Строковое значение
 */
case class JsonSchemaErrorTemplate() extends JsonSchemaTemplate {
  override def generateJValue(): JValue = {
    return JObject("type" -> JString("string"), "description" -> JString("Неопознанный тип"))
  }
}
