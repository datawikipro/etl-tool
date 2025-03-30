package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaNullTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def getTemplate: BaseSchemaTemplate = this
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    throw Exception()
  }
  
  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaBooleanTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaNullTemplate(inIsIgnorable)
      case _ => throw Exception()
  }
}
