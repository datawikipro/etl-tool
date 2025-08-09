package pro.datawiki.schemaValidator.jsonSchema

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JObject, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaObject

object JsonSchemaConstructor {

  private def getBaseSchemaFromJson(element: JsonSchemaElement): BaseSchemaStruct = {
    element match
      case x: JsonSchemaObject => return x.getBaseSchemaElementData
      case x: JsonSchemaArray => {
        if x.inList.length > 1 then throw SchemaValidationException("JSON array must contain exactly one element")

        x.inList.head match
          case y: JsonSchemaObject => return y.getBaseSchemaElementData
          case _ => throw SchemaValidationException("JSON array element must be an object")
      }
  }

  def getDataInBaseSchemaFormat(inDataJsonFormat: JValue): JsonSchemaElement = {
    val obj: JsonSchemaElement = inDataJsonFormat match
      case x: JArray => JsonSchemaArray.apply(x)
      case x: JObject => JsonSchemaObject.apply(x)
      case _ => throw SchemaValidationException(s"Unsupported JSON type: ${inDataJsonFormat.getClass.getSimpleName}")
    return obj
  }

  private def getDataFrameFromBaseSchema(dataJsonFormat: JValue, userSchemaTemplate: BaseSchemaTemplate): DataFrame = {
    val dataInJsonSchemaFormat: JsonSchemaElement = getDataInBaseSchemaFormat(dataJsonFormat)

    //TODO Сделать проверку что JSON меньше User схемы
    val dataInBaseSchemaFormat = getBaseSchemaFromJson(dataInJsonSchemaFormat)

    val result: BaseSchemaStruct = userSchemaTemplate.extractDataFromObject(dataInBaseSchemaFormat)

    result match
      case x: BaseSchemaObject => return x.packageDataFrame
      case _ => throw SchemaValidationException("Expected BaseSchemaObject result")
  }

  def getDataFrameFromWithSchemaObject(dataJsonFormat: JValue, userSchemaObject: SchemaObject): DataFrame = {
    val userSchemaTemplate: BaseSchemaTemplate = userSchemaObject.getBaseSchemaTemplate
    return getDataFrameFromBaseSchema(dataJsonFormat, userSchemaTemplate)
  }

  def getDataFrameFromWithOutSchemaObject(dataJsonFormat: JValue): DataFrame = {
    val dataInJsonSchemaFormat: JsonSchemaElement = getDataInBaseSchemaFormat(dataJsonFormat)
    return getDataFrameFromBaseSchema(dataJsonFormat, dataInJsonSchemaFormat.getBaseSchemaElementData.getTemplate)
  }


}
