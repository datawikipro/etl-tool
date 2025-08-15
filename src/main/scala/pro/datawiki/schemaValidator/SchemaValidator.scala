package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.DataType
import org.json4s.*
import org.json4s.jackson.JsonMethods
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate
import pro.datawiki.schemaValidator.json.JsonSchemaConstructor
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.yamlConfiguration.YamlClass

object SchemaValidator extends YamlClass {
  
  def validateDiffSchemas(inFirst:BaseSchemaTemplate, inSecond:BaseSchemaTemplate):Boolean={
    inFirst.leftMerge(inSecond)
    return true
  }
  
  def getSchemaFromJson(inJsonString: List[String]): DataType = {
    var schema: BaseSchemaTemplate = new JsonSchemaConstructor().getBaseSchemaTemplate(inJsonString)

    if (schema == null) {
      throw SchemaValidationException("Не удалось создать схему - возможно, входные JSON данные отсутствуют или все содержат ошибки")
    }

    val project = toYaml(schema.getProjectSchema)
    val template = schema.getSparkRowElementTemplate
    val resultType = template.getType
    return resultType
  }
  
  /**
   * Проверяет соответствие JSON-значения заданной схеме.
   *
   * @param loader Объект схемы для валидации
   * @param json JSON-значение для проверки
   * @return true, если JSON соответствует схеме, иначе false
   */
  def validateJsonBySchema(loader: SchemaObject, json: JValue): Boolean = {
    json match {
      case jsonObject: JObject => 
        loader.validateJson(jsonObject)

      case jsonArray: JArray if jsonArray.arr.length == 1 => 
        loader.validateJson(jsonArray.arr.head)

      case _ => false
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