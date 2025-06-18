package pro.datawiki.schemaValidator.jsonSchema

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JObject, JValue}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.projectSchema.SchemaObject

object JsonSchemaConstructor {

  private def getBaseSchemaFromJson(element: JsonSchemaElement): BaseSchemaStruct = {
    element match
      case x: JsonSchemaObject => return x.getBaseSchemaElementData
      case x: JsonSchemaArray => {
        if x.inList.length > 1 then throw Exception()

        x.inList.head match
          case y: JsonSchemaObject => return y.getBaseSchemaElementData
          case _ => throw Exception()
      }
  }

  def getDataInBaseSchemaFormat(inDataJsonFormat: JValue): JsonSchemaElement = {
    val obj: JsonSchemaElement = inDataJsonFormat match
      case x: JArray => JsonSchemaArray.apply(x)
      case x: JObject => JsonSchemaObject.apply(x)
      case _ => throw Exception()
    return obj
  }

  def getDataFrameFromBaseSchema(dataJsonFormat: JValue, userSchemaTemplate: BaseSchemaTemplate): DataFrame = {
    val dataInJsonSchemaFormat: JsonSchemaElement = getDataInBaseSchemaFormat(dataJsonFormat)

    //TODO Сделать проверку что JSON меньше User схемы
    val dataInBaseSchemaFormat = getBaseSchemaFromJson(dataInJsonSchemaFormat)

    val result: BaseSchemaStruct = userSchemaTemplate.extractDataFromObject(dataInBaseSchemaFormat)

    result match
      case x: BaseSchemaObject => return x.packageDataFrame
      case _ => throw Exception()
  }

  def getDataFrameFromWithSchemaObject(dataJsonFormat: JValue, userSchemaObject: SchemaObject): DataFrame = {
    val userSchemaTemplate: BaseSchemaTemplate = userSchemaObject.getBaseObject
    return getDataFrameFromBaseSchema(dataJsonFormat, userSchemaTemplate)
  }

  def getDataFrameFromWithOutSchemaObject(dataJsonFormat: JValue): DataFrame = {
    val dataInJsonSchemaFormat: JsonSchemaElement = getDataInBaseSchemaFormat(dataJsonFormat)
    return getDataFrameFromBaseSchema(dataJsonFormat, dataInJsonSchemaFormat.getBaseSchemaElementData.getTemplate)
  }


}
