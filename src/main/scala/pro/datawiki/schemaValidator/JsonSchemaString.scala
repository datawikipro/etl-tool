package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JString

class JsonSchemaString(in:String) extends JsonSchemaElement{

  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaString(in,false)
}

object JsonSchemaString {
  def apply(in:JString):JsonSchemaString = {
    return new JsonSchemaString(in.s)
  }
  
}
