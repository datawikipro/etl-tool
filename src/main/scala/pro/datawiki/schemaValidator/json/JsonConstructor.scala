package pro.datawiki.schemaValidator.json

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JNull, JObject, JValue}
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, string2JsonInput}
import pro.datawiki.datawarehouse.{DataFrameBaseSchema, DataFrameError, DataFrameTrait}
import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.*

class JsonConstructor extends Migration {

  override def extractStruct(inString: String): BaseSchemaStruct = {
    try {
      var str: String = inString
      val a = inString.substring(0, 1)
      if inString.substring(0, 1) == """"""" then
        str = str.substring(1, str.length - 1).replace("\\\"", "\"")

      val json: JValue = JsonMethods.parse(str)
      json match
        case x: JObject => JsonStruct(x).getBaseSchemaElementData
        case x: JArray => x.arr.length match {
          case 1 => {
            val b: JValue = x.arr.head
            b match {
              case y: JObject => JsonStruct(y).getBaseSchemaElementData
              case fs => {
                throw SchemaValidationException(s"Unsupported JSON array element type: ${fs.getClass.getName}")
              }
            }

          }
          case _ => {
            throw SchemaValidationException("JSON arrays with multiple elements are not supported")
          }
          case 0 => {
            BaseSchemaNull(true)
          }
          case fs => {
            throw SchemaValidationException(s"Unsupported JSON array length: $fs")
          }
        }
        case JNull => null
        case _ => {
          throw SchemaValidationException(s"Неподдерживаемый тип JSON: ${json.getClass.getSimpleName}")
        }
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def extractSchema(inString: String): BaseSchemaTemplate = {

    extractStruct(inString) match {
      case null => return BaseSchemaNullTemplate(true)
      case fs => return fs.getTemplate
    }
  }


  override def evolutionSchemaByObject(inJsonString: String, inSchema: BaseSchemaTemplate): BaseSchemaTemplate = {
    try {
      if (inJsonString == null || inJsonString.isEmpty) {
        throw Exception("Предупреждение: пустая или null JSON строка пропущена")
      }
      val dataInJsonSchemaFormat: BaseSchemaTemplate = extractSchema(inJsonString)

      if inSchema == null then return dataInJsonSchemaFormat
      return inSchema.fullMerge(dataInJsonSchemaFormat)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def validateSchemaForObject(inJsonString: String, inSchema: BaseSchemaTemplate = throw IllegalArgumentException("Schema is required")): Boolean = {
    try {
      if (inJsonString == null || inJsonString.isEmpty) {
        throw Exception("Предупреждение: пустая или null JSON строка пропущена")
      }
      val dataInJsonSchemaFormat: BaseSchemaTemplate = extractSchema(inJsonString)

      return inSchema.equals(dataInJsonSchemaFormat)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  def evolutionSchemaByList(inList: List[String], inSchema: BaseSchemaTemplate): BaseSchemaTemplate = {
    var schema: BaseSchemaTemplate = inSchema
    inList.foreach(i => {
      schema = evolutionSchemaByObject(i, schema)
    })
    return schema
  }

  override def readTemplate(inFileName: String): BaseSchemaTemplate = {
    return readSchema(inFileName).getTemplate
  }

  override def writeTemplate(inFileName: BaseSchemaTemplate): String = {
    val title: String = "Generated Schema"
    val description: String = "Автоматически сгенерированная JSON схема"
    JsonSchemaWriter.convertToJsonSchema(inFileName, title, description)
  }

  override def readSchema(inFileName: String): BaseSchemaStruct = {
    val configFile = java.nio.file.Paths.get(inFileName)
    if (!java.nio.file.Files.exists(configFile)) {
      return null
    }
    val json = JsonMethods.parse(inFileName)
    return JsonStruct(json).getBaseSchemaElementData
  }

  override def writeSchema(schema: BaseSchemaStruct): String = throw NotImplementedException("Method not implemented")

  override def getDataFrameFromString(inString: String,
                                      schemaName:String,
                                      inSchema: BaseSchemaTemplate,
                                      validData: Boolean,
                                      isStrongValidation: Boolean
                                     ): DataFrameTrait = {
    val a: BaseSchemaStruct = extractStruct(inString)

    if !inSchema.leftValidate(a.getTemplate) then {
      if !isStrongValidation then {
        return DataFrameError()
      }
      throw SchemaValidationException(s"Schema validation failed for JSON data. JSON string: ${inString.take(1000)}${if (inString.length > 1000) "..." else ""}")
    }

    a match {
      case x: BaseSchemaObject => {
        try {
          return DataFrameBaseSchema(schemaName, BaseSchemaArray(List.apply(x),inSchema, false),validData)
        } catch {
          case e: Exception => {
            throw e
          }
        }
      }
      case _ => {
        throw SchemaValidationException("Only BaseSchemaObject is supported for DataFrame creation")
      }
    }
  }
}