package pro.datawiki.schemaValidator

import org.json4s.JsonAST.*
import pro.datawiki.sparkLoader.LogMode

trait BaseSchemaStruct {
//  def equals(in:BaseSchemaStruct): Boolean
  //Базой является схема параметром данные
  //def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement
  def getSparkRowElement:SparkRowElementType
  def isIgnorable: Boolean
  def getTemplate: BaseSchemaTemplate

}
