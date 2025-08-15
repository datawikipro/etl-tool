package pro.datawiki.schemaValidator.json.jsonConfig

import org.json4s.{JObject, JString, JValue}
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.json.JsonSchemaWriter.generateJsonSchema
import pro.datawiki.schemaValidator.json.JsonSchemaWriter
import pro.datawiki.schemaValidator.json.JsonSchemaTemplate

import scala.collection.mutable

case class JsonSchemaArrayTemplate(schema: BaseSchemaArrayTemplate) extends JsonSchemaTemplate {

  private def itemSchema :JValue = schema.getBaseElement match {
    case objTemplate: BaseSchemaObjectTemplate => {
      val templateMap = mutable.Map[String, BaseSchemaTemplate]()
      objTemplate.getElements.foreach { case (name, template) =>
        templateMap += (name -> template)
      }
      JsonSchemaTemplate(new BaseSchemaObjectTemplate(templateMap, objTemplate.isIgnorable)).generateJValue()
    }
    case x: BaseSchemaStringTemplate  => JsonSchemaTemplate(new BaseSchemaStringTemplate(x.isIgnorable)).generateJValue()
    case x: BaseSchemaIntTemplate     => JsonSchemaTemplate(new BaseSchemaIntTemplate(x.isIgnorable)).generateJValue()
    case x: BaseSchemaDoubleTemplate  => JsonSchemaTemplate(new BaseSchemaDoubleTemplate(x.isIgnorable)).generateJValue()
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
