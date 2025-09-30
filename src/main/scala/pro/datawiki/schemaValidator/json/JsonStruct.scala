package pro.datawiki.schemaValidator.json

import org.json4s.JDouble
import org.json4s.JsonAST.*
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.jsonConfig.*


trait JsonStruct {
  def getBaseSchemaElementData: BaseSchemaStruct
  def isEmpty:Boolean
}

object JsonStruct {
  def apply(json: JValue): JsonStruct = {
    json match {
      case array: JArray => JsonSchemaArray(array)
      case obj: JObject => JsonSchemaObject(obj)
      case string: JString => JsonSchemaString(string)
      case int: JInt => JsonSchemaInt(int)
      case bool: JBool => JsonSchemaBoolean(bool)
      case double: JDouble => JsonSchemaDouble(double)
      case _ if json.toString == "JNull" => JsonSchemaNull()
      case _ => throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${json.getClass.getSimpleName}")
    }
  }

}
