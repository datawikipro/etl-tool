package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementString}

class BaseSchemaBoolean(in: Boolean,
                        inIsIgnorable: Boolean) extends BaseSchemaStruct {
  def getValue: Boolean = in

  def getSparkRowElement: SparkRowElement = SparkRowElementString(in.toString) //SparkRowElementBoolean(in)

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = BaseSchemaBooleanTemplate(inIsIgnorable)
}
