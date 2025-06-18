package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.JDouble
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaDouble, BaseSchemaStruct}

class JsonSchemaDouble(in: Double) extends JsonSchemaElement {

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaDouble(in, false)
}

object JsonSchemaDouble {
  def apply(in: JDouble): JsonSchemaDouble = {
    return new JsonSchemaDouble(in.num)
  }

}
