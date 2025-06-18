package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JDouble
import org.json4s.JsonAST.*
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaStruct
import pro.datawiki.schemaValidator.*

trait JsonSchemaElement() {
  def getBaseSchemaElementData: BaseSchemaStruct
  //  def getBaseSchemaElementTemplate:BaseSchemaTemplate
}

object JsonSchemaElement {
  def apply(json: JValue): JsonSchemaElement = {
    json match
      case x: JArray => JsonSchemaArray.apply(x)
      case x: JObject => JsonSchemaObject.apply(x)
      case x: JString => JsonSchemaString.apply(x)
      case x: JInt => JsonSchemaInt.apply(x)
      case x: JBool => JsonSchemaBoolean.apply(x)
      case x: JDouble => JsonSchemaDouble.apply(x)
      case _ => {
        if json.toString == "JNull" then return JsonSchemaNull.apply()
        throw Exception()
      }
      case _ => throw Exception()
  }

}
