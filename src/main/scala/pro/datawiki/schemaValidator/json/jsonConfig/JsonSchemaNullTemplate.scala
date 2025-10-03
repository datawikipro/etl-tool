package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate


case object JsonSchemaNullTemplate extends JsonSchemaTemplate {
  override def generateJValue(): JValue = {
    JObject("type" -> JString("null"))
  }
}
