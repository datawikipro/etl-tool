package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaStringTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x:BaseSchemaString => return BaseSchemaString(x.getValue,x.isIgnorable)
      case x:BaseSchemaInt => return BaseSchemaString(x.getValue.toString(),x.isIgnorable)
      case x:BaseSchemaBoolean => return BaseSchemaString(x.getValue.toString(),x.isIgnorable)
      case x:BaseSchemaNull => return BaseSchemaString("",x.isIgnorable)
      case _ => throw Exception()
  }
  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case _ => throw Exception()
  }
}
