package pro.datawiki.schemaValidator

import org.apache.hadoop.shaded.com.fasterxml.jackson.core.io.JsonEOFException
import org.apache.spark.sql.types.DataType
import org.json4s.*
import org.json4s.jackson.JsonMethods
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.jsonSchema.{JsonSchema, JsonSchemaConstructor}
import pro.datawiki.schemaValidator.jsonSchema.JsonSchemaConstructor.getDataInBaseSchemaFormat
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.yamlConfiguration.YamlClass

object SchemaValidator extends YamlClass {
  
  def validateDiffSchemas(inFirst:BaseSchemaTemplate, inSecond:BaseSchemaTemplate):Boolean={
    inFirst.leftMerge(inSecond)
    return true
  }
  
  def getSchemaFromJson(inJsonString: List[String]): DataType = {
    val jsonSchema =  JsonSchema(inJsonString)
    var schema: BaseSchemaTemplate = jsonSchema.getBaseSchemaTemplate  

    if (schema == null) {
      throw SchemaValidationException("Не удалось создать схему - возможно, входные JSON данные отсутствуют или все содержат ошибки")
    }

    val project = toYaml(schema.getProjectSchema)
    val template = schema.getSparkRowElementTemplate
    val resultType = template.getType
    return resultType
  }
  
  def validateJsonBySchema(loader: SchemaObject, json: JValue ) : Boolean = {
    json match
      case x: JObject => return loader.validateJson(x)
      case x: JArray => {
        if x.arr.length == 1 then return loader.validateJson(x.arr.head)
                             else return false
      }
      case _ => {
        return false
      }
  }

  def validateListJsonByTemplateAndGetDataType(inJsonString: List[String], validatorConfigLocation: String, updateSchema: Boolean):DataType = {
    val loader: SchemaObject = SchemaObject(validatorConfigLocation)
    inJsonString.foreach(i=> {
      try {
        val json: JValue = JsonMethods.parse(i)
        if !validateJsonBySchema(loader,json) then
          throw SchemaValidationException(s"Ошибка валидации JSON по схеме: $i")
      } catch {
        case e: SchemaValidationException => throw e // пробрасываем исходное исключение валидации
        case e: Exception => {
          // Анализ сообщения об ошибке для более точной классификации проблемы
          val errorMessage = e.getMessage.toLowerCase
          if (errorMessage.contains("parse") || errorMessage.contains("syntax")) {
            throw SchemaValidationException(s"Ошибка при парсинге JSON: $i - ${e.getMessage}", e)
          } else if (errorMessage.contains("mapping") || errorMessage.contains("convert")) {
            throw SchemaValidationException(s"Ошибка маппинга JSON: $i - ${e.getMessage}", e)
          } else {
            throw SchemaValidationException(s"Ошибка при валидации JSON: $i - ${e.getMessage}", e)
          }
        }
      }
    })

    val template = loader.getBaseSchemaTemplate.getSparkRowElementTemplate
    val resultType = template.getType

    return resultType
  }


}