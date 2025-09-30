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
case class JsonSchemaStringTemplate() extends JsonSchemaTemplate {
  /**
   * Генерирует схему для строкового типа.
   *
   * @param schema Объект BaseSchemaString
   * @return JValue, представляющий схему строки
   */
  override def generateJValue(): JValue = {
    JObject("type" -> JString("string"))
  }
}
