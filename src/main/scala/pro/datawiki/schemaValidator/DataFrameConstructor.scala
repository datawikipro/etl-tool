package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.json4s.{JArray, JObject, JValue}
import org.json4s.*
import org.json4s.jackson.JsonMethods
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.SchemaValidator.validateJsonBySchema
import pro.datawiki.schemaValidator.jsonSchema.JsonSchemaConstructor
import pro.datawiki.schemaValidator.projectSchema.SchemaObject

object DataFrameConstructor {

  def getDataFrameFromJsonWithOutTemplate(inJsonString: String): DataFrame = {
    try {
      val json: JValue = JsonMethods.parse(inJsonString)
      json match
        case x: JObject => return JsonSchemaConstructor.getDataFrameFromWithOutSchemaObject(json)
        case x: JArray => {
          if x.arr.length == 1 then {
            return JsonSchemaConstructor.getDataFrameFromWithOutSchemaObject(x.arr.head)
          } else throw SchemaValidationException("Массив JSON должен содержать ровно один элемент")
        }
        case _ => throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${json.getClass.getSimpleName}")
    } catch {
      case e: SchemaValidationException => throw e // пробрасываем исходное исключение валидации
      case e: Exception => {
        // Анализ сообщения об ошибке для более точной классификации проблемы
        val errorMessage = e.getMessage.toLowerCase
        if (errorMessage.contains("parse") || errorMessage.contains("syntax")) {
          throw SchemaValidationException(s"Ошибка при парсинге JSON для создания DataFrame без шаблона: ${e.getMessage}", e)
        } else if (errorMessage.contains("mapping") || errorMessage.contains("convert")) {
          throw SchemaValidationException(s"Ошибка маппинга JSON для создания DataFrame без шаблона: ${e.getMessage}", e)
        } else {
          throw SchemaValidationException(s"Ошибка при создании DataFrame из JSON без шаблона: ${e.getMessage}", e)
        }
      }
    }
  }

  def getDataFrameFromJsonWithTemplate(inJsonString: String, validatorConfigLocation: String): DataFrame = {
    try {
      val json: JValue = JsonMethods.parse(inJsonString)
      val loader: SchemaObject = SchemaObject(validatorConfigLocation)

      if !SchemaValidator.validateJsonBySchema(loader,json) then throw SchemaValidationException(s"Ошибка валидации JSON по схеме при создании DataFrame: $inJsonString")

      return JsonSchemaConstructor.getDataFrameFromWithSchemaObject(json, loader)
    } catch {
      case e: SchemaValidationException => throw e // пробрасываем исходное исключение валидации
      case e: Exception => {
        // Анализ сообщения об ошибке для более точной классификации проблемы
        val errorMessage = e.getMessage.toLowerCase
        if (errorMessage.contains("parse") || errorMessage.contains("syntax")) {
          throw SchemaValidationException(s"Ошибка при парсинге JSON для создания DataFrame: ${e.getMessage}", e)
        } else if (errorMessage.contains("mapping") || errorMessage.contains("convert")) {
          throw SchemaValidationException(s"Ошибка маппинга JSON для создания DataFrame: ${e.getMessage}", e)
        } else {
          throw SchemaValidationException(s"Ошибка при создании DataFrame из JSON с шаблоном: ${e.getMessage}", e)
        }
      }
    }
  }
}
