package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JBool
import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBoolean, BaseSchemaBooleanTemplate, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

/**
 * Представляет булево значение в JSON схеме.
 *
 * @param value Значение boolean.
 */
class JsonSchemaBooleanTemplate() extends JsonSchemaTemplate {
  /**
   * Генерирует схему для логического типа.
   *
   * @param schema Объект BaseSchemaBoolean
   * @return JValue, представляющий схему логического значения
   */
  override def generateJValue(): JValue = {
    JObject("type" -> JString("boolean"))
  }
}
