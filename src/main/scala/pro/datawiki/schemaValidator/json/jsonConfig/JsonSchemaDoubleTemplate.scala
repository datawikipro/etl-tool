package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JDouble
import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaDouble, BaseSchemaDoubleTemplate, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

/**
 * Представляет значение с плавающей точкой в JSON схеме.
 *
 * @param value Значение с плавающей точкой
 */
case class JsonSchemaDoubleTemplate() extends JsonSchemaTemplate {
  /**
   * Генерирует схему для типа с плавающей точкой.
   *
   * @param schema Объект BaseSchemaDouble
   * @return JValue, представляющий схему числа с плавающей точкой
   */
  override def generateJValue(): JValue = {
    JObject("type" -> JString("number"))
  }

}
