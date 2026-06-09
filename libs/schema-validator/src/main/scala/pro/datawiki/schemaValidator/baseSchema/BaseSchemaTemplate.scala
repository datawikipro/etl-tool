package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.SparkRowElementTypeTemplate

trait BaseSchemaTemplate {
  def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct

  def leftValidate(in: BaseSchemaTemplate): Boolean
  
  def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate 

  def equals(in: BaseSchemaTemplate): Boolean

  def getSparkRowElementTemplate: SparkRowElementTypeTemplate
  def getSparkRowElement(data: BaseSchemaStruct):SparkRowElement
  def getProjectSchema: SchemaTrait

  def isIgnorable: Boolean
}
//
//object BaseSchemaTemplate {
//  def apply(in: String): BaseSchemaTemplate = {
//
//    in match
//      case "String" => return BaseSchemaStringTemplate(false)
//      case "Int" => return BaseSchemaIntTemplate(false)
//      case "Long" => return BaseSchemaIntTemplate(false)
//      case "Boolean" => return BaseSchemaBooleanTemplate(false)
//      case _ => {
//        throw NotImplementedException("Method not implemented")
//      }
//  }
//}