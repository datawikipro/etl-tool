package pro.datawiki.schemaValidator.json

import org.json4s.JValue
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.*
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.json.jsonConfig.*

/**
 * Базовый интерфейс для всех элементов JSON схемы.
 * Предоставляет методы для преобразования JSON элементов в структуры базовой схемы.
 */
trait JsonSchemaTemplate {
  def generateJValue(): JValue
}

object JsonSchemaTemplate {
  def apply(json: BaseSchemaTemplate): JsonSchemaTemplate = {
    json match {
      case x: BaseSchemaObjectTemplate => JsonSchemaObjectTemplate(x)
      case x: BaseSchemaArrayTemplate => JsonSchemaArrayTemplate(x)
      case x: BaseSchemaStringTemplate => JsonSchemaStringTemplate()
      case x: BaseSchemaIntTemplate => JsonSchemaIntTemplate()
      case x: BaseSchemaDoubleTemplate => JsonSchemaDoubleTemplate()
      case x: BaseSchemaBooleanTemplate => JsonSchemaBooleanTemplate()
      case x: BaseSchemaNullTemplate => JsonSchemaNullTemplate
      case _ => JsonSchemaErrorTemplate()
    }
  }

}
