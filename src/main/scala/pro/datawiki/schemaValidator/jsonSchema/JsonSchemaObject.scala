package pro.datawiki.schemaValidator.jsonSchema

import org.json4s.JsonAST.JObject
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaStruct, BaseSchemaTemplate}

import scala.collection.mutable

class JsonSchemaObject(inElements: mutable.Map[String, JsonSchemaElement]) extends JsonSchemaElement {
  var elements: mutable.Map[String, JsonSchemaElement] = inElements

  override def getBaseSchemaElementData: BaseSchemaStruct = {
    var result: mutable.Map[String, BaseSchemaStruct] = mutable.Map()
    var resultTempl: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()
    elements.foreach(i => {
      val a = i._2.getBaseSchemaElementData
      result += (i._1, a)
      resultTempl += (i._1, a.getTemplate)
    })
    return BaseSchemaObject(result, resultTempl, false)
  }

  def mergeSchema(schemaObject: BaseSchemaObject): BaseSchemaObject = {
    throw Exception()
  }

}

object JsonSchemaObject {
  def apply(json: JObject): JsonSchemaObject = {
    var elements: mutable.Map[String, JsonSchemaElement] = mutable.Map()
    json.obj.foreach(i => elements += (i._1, JsonSchemaElement.apply(i._2)))
    return new JsonSchemaObject(elements)

  }

}

