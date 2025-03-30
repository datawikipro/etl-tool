package pro.datawiki.schemaValidator

import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.datawarehouse.DataFrameOriginal
import pro.datawiki.sparkLoader.YamlClass
import org.apache.spark.sql.{Column, DataFrame}
import scala.collection.mutable

object SchemaValidator extends YamlClass {

  def getDataFrameFromJson(inJsonString: String, validatorConfigLocation: String): DataFrame = {
    val json: JValue = parse(inJsonString)

    val loader: SchemaObject = mapper.readValue(getLines(validatorConfigLocation), classOf[SchemaObject])
    
    if !(json match
      case x: JObject => loader.validateJson(json)
      case x: JArray => if x.arr.length == 1 then loader.validateJson(x.arr.head) else throw Exception()
      case _ => throw Exception()
      ) then throw Exception()
    return JsonSchemaConstructor.getDataFrame(json, loader)

  }
}

