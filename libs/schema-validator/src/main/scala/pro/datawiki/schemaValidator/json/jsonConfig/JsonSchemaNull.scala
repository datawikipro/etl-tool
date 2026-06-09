package pro.datawiki.schemaValidator.json.jsonConfig

import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaNull, BaseSchemaStruct}
import pro.datawiki.schemaValidator.json.JsonStruct

/**
 * Представляет значение null в JSON схеме.
 */
case class JsonSchemaNull() extends JsonStruct {
  override def getBaseSchemaElementData: BaseSchemaStruct = BaseSchemaNull(false)

  override def isEmpty: Boolean = true
}

object JsonSchemaNull{
  def apply(): JsonSchemaNull=  {
    new JsonSchemaNull
  }
}
