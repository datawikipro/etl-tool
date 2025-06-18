package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaInt, BaseSchemaStruct}

class JsonSchemaInt(in: BigInt) extends JsonSchemaElement {

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaInt(in, false)
}

object JsonSchemaInt {
  def apply(in: JInt): JsonSchemaInt = {
    return new JsonSchemaInt(in.num)
  }

}
