package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.JString
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaNull, BaseSchemaStruct}

class JsonSchemaNull() extends JsonSchemaElement {
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaNull(false)
}

object JsonSchemaNull {
  def apply(): JsonSchemaNull = {
    return new JsonSchemaNull()
  }

}
