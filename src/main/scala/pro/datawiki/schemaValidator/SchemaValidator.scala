package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.jsonSchema.JsonSchemaConstructor
import pro.datawiki.schemaValidator.jsonSchema.JsonSchemaConstructor.getDataInBaseSchemaFormat
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.yamlConfiguration.YamlClass

object SchemaValidator extends YamlClass {

  def getSchemaFromJson(inJsonString: List[String]): DataType = {
    var schema: BaseSchemaTemplate = null
    inJsonString.foreach(i => {
      try {
        var str = i
        val a = i.substring(0,1)
        if i.substring(0,1) == """"""" then
          str = str.substring(1,str.length-1).replace("\\\"","\"")
        val json: JValue = parse(str)
        json match
          case x: JObject => {
            val dataInJsonSchemaFormat: BaseSchemaStruct = JsonSchemaConstructor.getDataInBaseSchemaFormat(json).getBaseSchemaElementData
            if schema == null then schema = dataInJsonSchemaFormat.getTemplate
            schema = schema.fullMerge(dataInJsonSchemaFormat.getTemplate)
          }
          case JNull => {}
          case _ => throw Exception()
      }
      catch
        case _ =>
          println(s"Not parsed json: $i")
    })

    val project =  toYaml(schema.getProjectSchema)

    val template = schema.getSparkRowElementTemplate
    val resultType = template.getType
    return resultType
  }

  private def getLoader(validatorConfigLocation:String):SchemaObject = {
    return  mapper.readValue(getLines(validatorConfigLocation), classOf[SchemaObject])
  }

  private def validateJsonBySchema(loader: SchemaObject, json: JValue ) : Boolean = {
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
    val loader: SchemaObject = getLoader(validatorConfigLocation)
    inJsonString.foreach(i=> {
      val json: JValue = parse(i)
      if !validateJsonBySchema(loader,json) then
        throw Exception()
    })

    val template = loader.getBaseObject.getSparkRowElementTemplate
    val resultType = template.getType

    return resultType

  }

  def getDataFrameFromJsonWithTemplate(inJsonString: String, validatorConfigLocation: String): DataFrame = {
    val json: JValue = parse(inJsonString)

    val loader: SchemaObject = getLoader(validatorConfigLocation)

    if !validateJsonBySchema(loader,json) then throw Exception()

    return JsonSchemaConstructor.getDataFrameFromWithSchemaObject(json, loader)
  }

  def getDataFrameFromJsonWithOutTemplate(inJsonString: String): DataFrame = {
    val json: JValue = parse(inJsonString)
    json match
      case x: JObject => return JsonSchemaConstructor.getDataFrameFromWithOutSchemaObject(json)
      case x: JArray => {
        if x.arr.length == 1 then {
          return JsonSchemaConstructor.getDataFrameFromWithOutSchemaObject(x.arr.head)
        } else throw Exception()
      }
      case _ => throw Exception()
  }

}