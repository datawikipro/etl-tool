package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

/**
 * Представляет значение null в JSON схеме.
 */
case object JsonSchemaNullTemplate extends JsonSchemaTemplate {
  /**
   * Генерирует схему для null типа.
   *
   * @param schema Объект BaseSchemaNull
   * @return JValue, представляющий схему null
   */
  override def generateJValue(): JValue = {
    JObject("type" -> JString("null"))
  }
}
