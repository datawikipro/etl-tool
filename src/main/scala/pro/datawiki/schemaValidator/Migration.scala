package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.json.JsonConstructor
import pro.datawiki.schemaValidator.projectSchema.ProjectSchemaConstructor
import pro.datawiki.yamlConfiguration.YamlClass

trait Migration {
  def readTemplate(inFileName: String): BaseSchemaTemplate

  def writeTemplate(schema: BaseSchemaTemplate): String

  def writeTemplate(inFileName: String, schema: BaseSchemaTemplate): Boolean = {
    YamlClass.writefile(inFileName, writeTemplate(schema))
    return true
  }

  def readSchema(inFileName: String): BaseSchemaStruct

  def writeSchema(inFileName: BaseSchemaStruct): String

  def extractStruct(inString: String): BaseSchemaStruct

  def extractSchema(inString: String): BaseSchemaTemplate

  def evolutionSchemaByObject(inString: String, inSchema: BaseSchemaTemplate): BaseSchemaTemplate
  
  def validateSchemaForObject(inString: String, inSchema: BaseSchemaTemplate): Boolean

  def evolutionSchemaByList(inList: List[String], inSchema: BaseSchemaTemplate): BaseSchemaTemplate

  def getDataFrameFromString(inString: String,
                             schemaName:String, 
                             inSchema: BaseSchemaTemplate,
                             validData: Boolean,
                             isStrongValidation: Boolean): DataFrameTrait


}

object Migration {
  def apply(in: String): Migration = {
    in match {
      case "avro" => throw NotImplementedException("Avro migration not implemented")
      case "dataFrame" => throw NotImplementedException("DataFrame migration not implemented")
      case "json" => JsonConstructor()
      case "jsonSchema" => JsonConstructor()
      case "projectSchema" => ProjectSchemaConstructor()
      case "sparkRow" => throw NotImplementedException("SparkRow migration not implemented")
      case _ => throw NotImplementedException(s"Migration system '$in' not implemented")
    }
  }
}
