package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementInt}

class BaseSchemaInt(in: Long,
                    inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaIntTemplate(inIsIgnorable)

  def getValue: Long = in

  override def getSparkRowElement: SparkRowElement = SparkRowElementInt(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
