package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.json4s.{JArray, JObject, JValue}
import org.json4s.*
import org.json4s.jackson.JsonMethods
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.SchemaValidator.validateJsonBySchema
import pro.datawiki.schemaValidator.json.JsonSchemaConstructor
import pro.datawiki.schemaValidator.projectSchema.SchemaObject

object DataFrameConstructor {

  /**
   * Создает DataFrame из JSON строки без использования предопределенного шаблона.
   * Схема будет определена автоматически на основе структуры JSON.
   *
   * @param jsonString JSON строка для преобразования в DataFrame
   * @return DataFrame, созданный из JSON строки
   * @throws SchemaValidationException при ошибках валидации или обработки JSON
   */
  def getDataFrameFromJsonWithOutTemplate(jsonString: String): DataFrame = {
    try {
      // Разбор JSON строки
      val json = JsonMethods.parse(jsonString)

      // Преобразование в DataFrame в зависимости от типа JSON
      json match {
        case jsonObject: JObject => 
          new JsonSchemaConstructor().getDataFrameFromWithOutSchemaObject(json)

        case jsonArray: JArray if jsonArray.arr.length == 1 =>
          new JsonSchemaConstructor().getDataFrameFromWithOutSchemaObject(jsonArray.arr.head)

        case jsonArray: JArray => 
          throw SchemaValidationException("Массив JSON должен содержать ровно один элемент")

        case _ => 
          throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${json.getClass.getSimpleName}")
      }
    } catch {
      case e: SchemaValidationException => throw e // пробрасываем исходное исключение валидации
      case e: Exception => handleJsonParsingError(e, "без шаблона", jsonString)
    }
  }

  /**
   * Создает DataFrame из JSON строки с использованием предопределенного шаблона схемы.
   *
   * @param jsonString JSON строка для преобразования в DataFrame
   * @param validatorConfigLocation Путь к конфигурации валидатора схемы
   * @return DataFrame, созданный из JSON строки
   * @throws SchemaValidationException при ошибках валидации или обработки JSON
   */
  def getDataFrameFromJsonWithTemplate(jsonString: String, validatorConfigLocation: String): DataFrame = {
    try {
      // Разбор JSON строки
      val json = JsonMethods.parse(jsonString)

      // Загрузка схемы из конфигурации
      val loader = SchemaObject(validatorConfigLocation)

      // Валидация JSON по схеме
      if (!SchemaValidator.validateJsonBySchema(loader, json)) {
        throw SchemaValidationException(s"Ошибка валидации JSON по схеме при создании DataFrame: $jsonString")
      }

      // Преобразование в DataFrame с использованием схемы
      new JsonSchemaConstructor().getDataFrameFromWithSchemaObject(json, loader)
    } catch {
      case e: SchemaValidationException => throw e // пробрасываем исходное исключение валидации
      case e: Exception => handleJsonParsingError(e, "с шаблоном", jsonString)
    }
  }

  /**
   * Вспомогательный метод для обработки ошибок при парсинге JSON.
   * Анализирует сообщение об ошибке и генерирует соответствующее исключение с информативным сообщением.
   *
   * @param exception Исходное исключение
   * @param contextDescription Описание контекста обработки (например, "с шаблоном" или "без шаблона")
   * @param jsonString Исходная JSON строка, вызвавшая ошибку
   * @return Никогда не возвращает значение, всегда выбрасывает исключение
   * @throws SchemaValidationException с детальным описанием проблемы
   */
  private def handleJsonParsingError(exception: Exception, contextDescription: String, jsonString: String): Nothing = {
    val errorMessage = exception.getMessage.toLowerCase

    // Определяем тип ошибки по содержимому сообщения
    val specificMessage = if (errorMessage.contains("parse") || errorMessage.contains("syntax")) {
      s"Ошибка при парсинге JSON для создания DataFrame $contextDescription: ${exception.getMessage}"
    } else if (errorMessage.contains("mapping") || errorMessage.contains("convert")) {
      s"Ошибка маппинга JSON для создания DataFrame $contextDescription: ${exception.getMessage}"
    } else {
      s"Ошибка при создании DataFrame из JSON $contextDescription: ${exception.getMessage}"
    }

    throw SchemaValidationException(specificMessage, exception)
  }
}
