package pro.datawiki.schemaValidator.jsonSchema


import org.apache.hadoop.shaded.com.fasterxml.jackson.core.io.JsonEOFException
import org.json4s.*
import org.json4s.jackson.JsonMethods
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.jsonSchema.JsonSchemaConstructor.getDataInBaseSchemaFormat


class JsonSchema(schema: BaseSchemaTemplate) {

  def getBaseSchemaTemplate:BaseSchemaTemplate = {
    return schema
  }
}

object JsonSchema {
  def apply(inJsonString: List[String]):JsonSchema={
    var schema: BaseSchemaTemplate = null
    inJsonString.foreach(i => {
      try {
        if (i == null || i.isEmpty) {
          throw Exception("Предупреждение: пустая или null JSON строка пропущена")
        }
        var str = i
        val a = i.substring(0,1)
        if i.substring(0,1) == """"""" then
          str = str.substring(1,str.length-1).replace("\\\"","\"")
        val json: JValue = JsonMethods.parse(str)
        json match
          case x: JObject => {
            val dataInJsonSchemaFormat: BaseSchemaStruct = JsonSchemaConstructor.getDataInBaseSchemaFormat(json).getBaseSchemaElementData
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
        case e:Exception =>{
          //          throw SchemaValidationException(s"Failed to parse JSON: $i", e)
          println(s"Ошибка при разборе JSON: $i - ${e.getMessage}") //TODO: заменить на логирование
          if (schema == null) {
            throw SchemaValidationException("Не удалось создать схему из JSON - все образцы содержали ошибки", e)
          }
        }
    })
    return new JsonSchema(schema)
  }
}