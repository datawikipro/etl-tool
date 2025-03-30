package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}

class JsonSchemaInt(in:BigInt) extends JsonSchemaElement{

  override def getBaseSchemaElementData: BaseSchemaStruct =  BaseSchemaInt(in,false)
}

object JsonSchemaInt {
  def apply(in:JInt):JsonSchemaInt = {
    return new JsonSchemaInt(in.num)
  }
  
}
