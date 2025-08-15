package pro.datawiki.schemaValidator.json

import com.fasterxml.jackson.core.io.JsonEOFException
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JNull, JObject, JValue}
import org.json4s.jackson.JsonMethods
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import org.json4s.string2JsonInput
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.json.jsonConfig.{JsonSchemaArray, JsonSchemaObject}

class JsonSchemaConstructor extends Migration{

  private def getBaseSchemaFromJson(element: JsonStruct): BaseSchemaStruct = {
    element match {
      case jsonObj: JsonSchemaObject => 
        return jsonObj.getBaseSchemaElementData

      case jsonArray: JsonSchemaArray => 
        if (jsonArray.inList.length > 1) {
          throw SchemaValidationException("JSON массив должен содержать ровно один элемент")
        }

        jsonArray.inList.head match {
          case jsonObj: JsonSchemaObject => return jsonObj.getBaseSchemaElementData
          case _ => throw SchemaValidationException("Элемент JSON массива должен быть объектом")
        }
    }
  }

  def getDataInBaseSchemaFormat(jsonValue: JValue): JsonStruct = {
    jsonValue match {
      case jArray: JArray => JsonSchemaArray(jArray)
      case jObject: JObject => JsonSchemaObject(jObject)
      case _ => throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${jsonValue.getClass.getSimpleName}")
    }
  }

  private def getDataFrameFromBaseSchema(jsonData: JValue, schemaTemplate: BaseSchemaTemplate): DataFrame = {
    // Преобразуем JSON в формат JsonSchemaElement
    val jsonSchemaElement = getDataInBaseSchemaFormat(jsonData)

    // TODO: Добавить проверку, что JSON структура не превышает по размеру схему пользователя

    // Получаем базовую структуру схемы из JSON элемента
    val baseSchemaStruct = getBaseSchemaFromJson(jsonSchemaElement)

    // Извлекаем данные с использованием шаблона схемы
    val result = schemaTemplate.extractDataFromObject(baseSchemaStruct)

    // Преобразуем результат в DataFrame
    result match {
      case schemaObject: BaseSchemaObject => schemaObject.packageDataFrame
      case _ => throw SchemaValidationException("Ожидался результат типа BaseSchemaObject")
    }
  }

  def getDataFrameFromWithSchemaObject(jsonData: JValue, schemaObject: SchemaObject): DataFrame = {
    val schemaTemplate = schemaObject.getBaseSchemaTemplate
    getDataFrameFromBaseSchema(jsonData, schemaTemplate)
  }

  def getDataFrameFromWithOutSchemaObject(jsonData: JValue): DataFrame = {
    val jsonSchemaElement = getDataInBaseSchemaFormat(jsonData)
    val derivedTemplate = jsonSchemaElement.getBaseSchemaElementData.getTemplate
    getDataFrameFromBaseSchema(jsonData, derivedTemplate)
  }

  def getBaseSchemaTemplate(inJsonString: List[String]): BaseSchemaTemplate = {
    var schema: BaseSchemaTemplate = null
    inJsonString.foreach(i => {
      try {
        if (i == null || i.isEmpty) {
          throw Exception("Предупреждение: пустая или null JSON строка пропущена")
        }
        var str: String = i
        val a = i.substring(0, 1)
        if i.substring(0, 1) == """"""" then
          str = str.substring(1, str.length - 1).replace("\\\"", "\"")
        
        val json: JValue = JsonMethods.parse(str)
        

        json match
          case x: JObject => {
            val dataInJsonSchemaFormat: BaseSchemaStruct = getDataInBaseSchemaFormat(json).getBaseSchemaElementData
            if schema == null then schema = dataInJsonSchemaFormat.getTemplate
            schema = schema.fullMerge(dataInJsonSchemaFormat.getTemplate)
          }
          case JNull => {}
          case _ => throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${json.getClass.getSimpleName}")
      }
      catch
        case e: SchemaValidationException => {
          val errorText = s"Ошибка валидации схемы при разборе JSON: $i - ${e.getMessage}"
          //          if LogMode.isDebug then throw SchemaValidationException(s"Failed to parse JSON: $i", e)
          println(errorText) //TODO: заменить на логирование
        }
        case e: JsonEOFException => {
          val errorText = s"Ошибка EOF при разборе JSON: $i - ${e.getMessage}"
          //          if LogMode.isDebug then throw SchemaValidationException(s"Failed to parse JSON: $i", e)
          println(errorText) //TODO: заменить на логирование
        }
        case e: Exception => {
          //          throw SchemaValidationException(s"Failed to parse JSON: $i", e)
          println(s"Ошибка при разборе JSON: $i - ${e.getMessage}") //TODO: заменить на логирование
          if (schema == null) {
            throw SchemaValidationException("Не удалось создать схему из JSON - все образцы содержали ошибки", e)
          }
        }
    })
    return schema
  }

  override def readTemplate(in: String): BaseSchemaTemplate = {
    return readSchema(in).getTemplate
  }

  override def writeTemplate(in: BaseSchemaTemplate): String = {
    val title: String = "Generated Schema"
    val description: String = "Автоматически сгенерированная JSON схема"
    JsonSchemaWriter.convertToJsonSchema(in, title, description)
  }

  override def readSchema(in: String): BaseSchemaStruct = {
    val json = JsonMethods.parse(in)
    return getDataInBaseSchemaFormat(json).getBaseSchemaElementData
  }

  override def writeSchema(in: BaseSchemaStruct): String = throw Exception()
}
