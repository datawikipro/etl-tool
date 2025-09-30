package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementString}

class BaseSchemaString(in: String,
                       inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaStringTemplate(inIsIgnorable)

  def getValue: String = in

  def getSparkRowElement: SparkRowElement = SparkRowElementString(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
