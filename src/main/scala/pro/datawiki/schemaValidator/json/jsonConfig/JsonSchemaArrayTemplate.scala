package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.json.JsonSchemaWriter.generateJsonSchema
import pro.datawiki.schemaValidator.json.{JsonSchemaTemplate, JsonSchemaWriter}

import scala.collection.mutable

case class JsonSchemaArrayTemplate(schema: BaseSchemaArrayTemplate) extends JsonSchemaTemplate {

  private def itemSchema: JValue = schema.getBaseElement match {
    case x: BaseSchemaObjectTemplate => JsonSchemaTemplate(new BaseSchemaObjectTemplate(x.inElements, x.isIgnorable)).generateJValue()
    case x: BaseSchemaStringTemplate => JsonSchemaTemplate(new BaseSchemaStringTemplate(x.isIgnorable)).generateJValue()
    case x: BaseSchemaIntTemplate => JsonSchemaTemplate(new BaseSchemaIntTemplate(x.isIgnorable)).generateJValue()
    case x: BaseSchemaDoubleTemplate => JsonSchemaTemplate(new BaseSchemaDoubleTemplate(x.isIgnorable)).generateJValue()
    case x: BaseSchemaBooleanTemplate => JsonSchemaTemplate(new BaseSchemaBooleanTemplate(x.isIgnorable)).generateJValue()

    case _ => JObject("type" -> JString("string"))
  }

  override def generateJValue(): JValue = {
    JObject(
      "type" -> JString("array"),
      "items" -> itemSchema
    )
  }
}
