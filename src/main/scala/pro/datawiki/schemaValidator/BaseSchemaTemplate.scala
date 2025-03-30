package pro.datawiki.schemaValidator

import org.json4s.JsonAST.*
import pro.datawiki.sparkLoader.LogMode

trait BaseSchemaTemplate {
  def extractDataFromObject(dataElement:BaseSchemaStruct):BaseSchemaStruct
  def leftMerge(in:BaseSchemaTemplate): BaseSchemaTemplate
  def rightMerge(in:BaseSchemaTemplate): BaseSchemaTemplate= in.leftMerge(this)
  def fullMerge(in:BaseSchemaTemplate): BaseSchemaTemplate = this.rightMerge(this.leftMerge(in))
  def equals(in:BaseSchemaTemplate): BaseSchemaTemplate = throw Exception()

  def getTemplate: BaseSchemaTemplate = this
}

