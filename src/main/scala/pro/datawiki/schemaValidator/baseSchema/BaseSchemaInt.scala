package pro.datawiki.schemaValidator.baseSchema

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementInt, SparkRowElementType}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaInt(in: BigInt,
                    inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaIntTemplate(inIsIgnorable)
  def getValue: BigInt = in

  override def getSparkRowElement: SparkRowElementType = SparkRowElementInt(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
