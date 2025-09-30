package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement

trait BaseSchemaStruct {

  def getSparkRowElement: SparkRowElement

  def isIgnorable: Boolean

  def getTemplate: BaseSchemaTemplate

}


object BaseSchemaStruct {

}