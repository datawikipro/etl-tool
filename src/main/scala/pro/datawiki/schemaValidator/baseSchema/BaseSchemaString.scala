package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementString, SparkRowElementType}
import pro.datawiki.sparkLoader.LogMode

class BaseSchemaString(in: String,
                       inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaStringTemplate(inIsIgnorable)
  def getValue: String = in

  def getSparkRowElement: SparkRowElementType = SparkRowElementString(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
