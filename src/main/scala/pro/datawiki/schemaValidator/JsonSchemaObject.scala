package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JObject

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

  //
  //  override def getBaseSchemaElementTemplate: BaseSchemaTemplate = {
  //    var result: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()
  //    elements.foreach(i => {
  //      result += (i._1, i._2.getBaseSchemaElementTemplate)
  //    })
  //
  //    throw Exception()//return BaseSchemaObject.applyTemplate(result, false)
  //  }
  //
  def mergeSchema(schemaObject: BaseSchemaObject): BaseSchemaObject = {
    throw Exception()
    //    //Проверяем что в JSON нет новых столбцов
    //    elements.foreach(i=> {
    //      val result = schemaObject.checkElementExists(i._1,i._2.getBaseSchemaElementTemplate)
    //      if !result then
    //        throw Exception()
    //    })
    //
    //    var baseSchemaElements: mutable.Map[String, BaseSchemaElement] = mutable.Map()
    //
    //    schemaObject.getElements.foreach(i=> {
    //      elements.get(i._1) match
    //        case Some(x)=> baseSchemaElements += (i._1, i._2.mergeSchema(x.getBaseSchemaElementData))
    //        case _ => //TODO
    //    })
    //    return BaseSchemaObject(baseSchemaElements,schemaObject.isIgnorable)

  }

}

object JsonSchemaObject {
  def apply(json: JObject): JsonSchemaObject = {
    var elements: mutable.Map[String, JsonSchemaElement] = mutable.Map()
    json.obj.foreach(i => elements += (i._1, JsonSchemaElement.apply(i._2)))
    return new JsonSchemaObject(elements)

  }

}

