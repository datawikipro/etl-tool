package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JObject
import org.json4s.{JArray, JBool, JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonSchemaWriter.generateJsonSchema
import pro.datawiki.schemaValidator.json.JsonSchemaWriter
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

import scala.collection.mutable

/**
 * Представляет JSON объект в виде коллекции пар ключ-значение,
 * где значения являются элементами JSON схемы.
 *
 * @param elements Карта элементов JSON объекта (поле-значение)
 */
class JsonSchemaObjectTemplate(schema: BaseSchemaObjectTemplate) extends JsonSchemaTemplate {
  /**
   * Генерирует схему для объектного типа.
   *
   * @param schema Объект BaseSchemaObject
   * @return JValue, представляющий схему объекта
   */
  override def generateJValue(): JValue = {
    val properties = JObject(schema.getElements.map { case (name, elemSchema) =>
      name -> JsonSchemaWriter.generateJsonSchema(elemSchema, null, null, false)
    }.toList)

    val required = JArray(schema.getElements.filter { case (_, elemSchema) =>
      !elemSchema.isIgnorable
    }.map { case (name, _) =>
      JString(name)
    }.toList)

    JObject(
      "type" -> JString("object"),
      "properties" -> properties,
      "required" -> required,
      "additionalProperties" -> JBool(false)
    )
  }

}
