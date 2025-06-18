package pro.datawiki.schemaValidator.baseSchema

import org.json4s.JsonAST.JString
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementString, SparkRowElementType}
import pro.datawiki.sparkLoader.LogMode

case class BaseSchemaNull(inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaNullTemplate(inIsIgnorable)
  //  override def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement = {
  //    schemaObject match
  //      case x: BaseSchemaNull => return x
  //      case x: BaseSchemaString => {
  //        if x.in == null then
  //          return BaseSchemaString("",inIsIgnorable)
  //        throw Exception()
  //      }
  //      case _ => throw Exception()
  //  }

  def getSparkRowElement: SparkRowElementType = SparkRowElementString("")

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
