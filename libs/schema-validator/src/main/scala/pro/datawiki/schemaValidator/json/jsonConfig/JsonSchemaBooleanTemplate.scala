package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.JsonAST.JBool
import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBoolean, BaseSchemaBooleanTemplate, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate


class JsonSchemaBooleanTemplate() extends JsonSchemaTemplate {
  override def generateJValue(): JValue = {
    JObject("type" -> JString("boolean"))
  }
}
