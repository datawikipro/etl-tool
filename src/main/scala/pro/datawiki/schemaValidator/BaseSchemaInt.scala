package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaInt(in: BigInt,
                    inIsIgnorable: Boolean) extends BaseSchemaIntTemplate(inIsIgnorable), BaseSchemaStruct {

  def getValue: BigInt = in
  
  override def getSparkRowElement: SparkRowElementType = SparkRowElementInt(in)

  override def isIgnorable: Boolean = inIsIgnorable
}
