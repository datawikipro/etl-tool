package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.sparkRow.SparkRowElementType

trait BaseSchemaStruct {
  //def equals(in:BaseSchemaStruct): Boolean
  //Базой является схема параметром данные
  //def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement
  def getSparkRowElement: SparkRowElementType

  def isIgnorable: Boolean

  def getTemplate: BaseSchemaTemplate

}
