package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JBool

class JsonSchemaBoolean(in:Boolean) extends JsonSchemaElement{

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaBoolean(in,false)
}

object JsonSchemaBoolean {
  def apply(in:JBool):JsonSchemaBoolean = {
    return new JsonSchemaBoolean(in.value)
  }
  
}
