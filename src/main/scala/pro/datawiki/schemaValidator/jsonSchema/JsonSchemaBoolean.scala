package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.JBool
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaBoolean, BaseSchemaStruct}

class JsonSchemaBoolean(in: Boolean) extends JsonSchemaElement {

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaBoolean(in, false)
}

object JsonSchemaBoolean {
  def apply(in: JBool): JsonSchemaBoolean = {
    return new JsonSchemaBoolean(in.value)
  }

}
