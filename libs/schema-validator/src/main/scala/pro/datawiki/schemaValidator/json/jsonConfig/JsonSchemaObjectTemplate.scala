package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JObject
import org.json4s.{JArray, JBool, JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonSchemaWriter.generateJsonSchema
import pro.datawiki.schemaValidator.json.{JsonSchemaTemplate, JsonSchemaWriter}

import scala.collection.mutable

/**
 * Представляет JSON объект в виде коллекции пар ключ-значение,
 * где значения являются элементами JSON схемы.
 *
 * @param elements Карта элементов JSON объекта (поле-значение)
 */
class JsonSchemaObjectTemplate(schema: BaseSchemaObjectTemplate) extends JsonSchemaTemplate {
  override def generateJValue(): JValue = {
    val properties = JObject(schema.inElements.map { case (name, elemSchema) =>
      name -> JsonSchemaWriter.generateJsonSchema(elemSchema, null, null, false)
    }.toList)

    val required = JArray(schema.inElements.filter { case (_, elemSchema) =>
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
