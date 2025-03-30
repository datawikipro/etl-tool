package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JString

class JsonSchemaNull() extends JsonSchemaElement{
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaNull(false)
}

object JsonSchemaNull {
  def apply():JsonSchemaNull = {
    return new JsonSchemaNull()
  }
  
}
