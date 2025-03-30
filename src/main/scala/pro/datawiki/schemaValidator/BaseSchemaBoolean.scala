package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JBool
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaBoolean(in:Boolean, 
                             inIsIgnorable:Boolean) extends BaseSchemaBooleanTemplate(inIsIgnorable), BaseSchemaStruct{
  
  def getValue:Boolean = in
  //
//  override def equals(in: BaseSchemaStruct): Boolean = {
//    in match
//      case x: BaseSchemaBoolean => return true
//      case _ => return LogMode.getDebugFalse
//  }
//TODO
//  override def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement = {
//    schemaObject match
//      case x: BaseSchemaBoolean => BaseSchemaString(in.toString,inIsIgnorable)//return x
//      case x: BaseSchemaString => BaseSchemaString("",inIsIgnorable)
//      case _ => throw Exception()
//  }

  def getSparkRowElement: SparkRowElementType = SparkRowElementString(in.toString)//SparkRowElementBoolean(in)

  override def isIgnorable: Boolean = inIsIgnorable
}
