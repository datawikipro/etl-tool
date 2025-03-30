package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaBooleanTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def getTemplate: BaseSchemaTemplate = this

  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x:BaseSchemaBoolean => return BaseSchemaBoolean(x.getValue,x.isIgnorable)
      case _ => throw Exception()
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x:BaseSchemaBooleanTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
      case x:BaseSchemaString => return BaseSchemaStringTemplate(inIsIgnorable)
      case _ => throw Exception()
  }
}
