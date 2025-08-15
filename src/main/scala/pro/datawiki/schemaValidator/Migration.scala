package pro.datawiki.schemaValidator

import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonSchemaConstructor

trait Migration {
  def readTemplate(in: String): BaseSchemaTemplate
  def writeTemplate(in: BaseSchemaTemplate): String
  def readSchema(in: String): BaseSchemaStruct
  def writeSchema(in: BaseSchemaStruct): String
}

object Migration {
    def getSystem(in: String): Migration = {
      in match {
        case "avro" => throw Exception()
        case "dataFrame"=> throw Exception()
        case "json"=> JsonSchemaConstructor()
        case "jsonSchema"=> JsonSchemaConstructor()
        case "projectSchema"=> throw Exception()
        case "sparkRow"=> throw Exception()
        case _ => throw Exception()
      }
    }
}
