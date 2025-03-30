package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JObject, JValue}

object JsonSchemaConstructor {

  
  private def getBaseSchemaFromJson(element:JsonSchemaElement): BaseSchemaStruct = {
    element match
      case x: JsonSchemaObject => return x.getBaseSchemaElementData
      case x: JsonSchemaArray => {
        if x.inList.length > 1 then throw Exception()

        x.inList.head match
          case y: JsonSchemaObject => return y.getBaseSchemaElementData
          case _ => throw Exception()
      }
  }

  private def getDataInBaseSchemaFromat(inDataJsonFormat: JValue):JsonSchemaElement ={
    val obj: JsonSchemaElement = inDataJsonFormat match
      case x: JArray => JsonSchemaArray.apply(x)
      case x: JObject => JsonSchemaObject.apply(x)
      case _ => throw Exception()
    
    return obj
  }
  
  
  def getDataFrame(dataJsonFormat: JValue, userSchemaObject: SchemaObject): DataFrame = {

    val dataInJsonSchemaFormat:JsonSchemaElement = getDataInBaseSchemaFromat(dataJsonFormat)
    val userSchemaTemplate:BaseSchemaTemplate = userSchemaObject.getBaseObject
    
    //TODO Сделать проверку что JSON меньше User схемы
    val dataInBaseSchemaFormat = getBaseSchemaFromJson(dataInJsonSchemaFormat)
    
    val result:BaseSchemaStruct = userSchemaTemplate.extractDataFromObject(dataInBaseSchemaFormat)
    
    result match
      case x: BaseSchemaObject=> return packageDataFrame(x)
      case _ => throw Exception()
    
  }
  
  private def packageDataFrame(in:BaseSchemaObject):DataFrame = {
    var attributes: List[SparkRowAttribute] = List.apply()

    in.getElements.foreach(i => {
      attributes = attributes.appended(SparkRowAttribute(i._1, i._2.getSparkRowElement))
    })
    val sparkRow: SparkRow = new SparkRow(attributes)
    return sparkRow.getDataFrame
  }

}
