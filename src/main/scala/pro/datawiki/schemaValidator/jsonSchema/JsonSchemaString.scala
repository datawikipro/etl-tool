package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.JString
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaString, BaseSchemaStruct}

class JsonSchemaString(in: String) extends JsonSchemaElement {

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaString(in, false)
}

object JsonSchemaString {
  def apply(in: JString): JsonSchemaString = {
    return new JsonSchemaString(in.s)
  }

}
