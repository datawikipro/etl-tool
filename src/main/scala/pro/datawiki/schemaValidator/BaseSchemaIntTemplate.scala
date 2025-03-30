package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaIntTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x:BaseSchemaInt => return BaseSchemaInt(x.getValue,x.isIgnorable)

      case _ => throw Exception()
  }
  
  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case _ => throw Exception()
  }
}
