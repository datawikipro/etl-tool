package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementDouble, SparkRowElementType}

class BaseSchemaDouble(in: Double,
                       inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaDoubleTemplate(inIsIgnorable)
  def getValue: Double = in

  override def getSparkRowElement: SparkRowElementType = SparkRowElementDouble(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
