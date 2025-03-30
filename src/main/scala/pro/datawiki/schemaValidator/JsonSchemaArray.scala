package pro.datawiki.schemaValidator

import org.json4s.JsonAST.JArray

case class JsonSchemaArray(inList: List[JsonSchemaElement]) extends JsonSchemaElement {
  //  override def getBaseSchemaElementTemplate: BaseSchemaTemplate = BaseSchemaArray(List.apply(), baseElementType, false)

  override def getBaseSchemaElementData: BaseSchemaStruct = {
    var listStruct: List[BaseSchemaStruct] = List.apply()
    var template: BaseSchemaTemplate = null
    inList.foreach(i => {
      val a: BaseSchemaStruct = i.getBaseSchemaElementData
      listStruct = listStruct.appended(a)
      if template == null then template = a.getTemplate
      template = template.fullMerge(a.getTemplate)
    })
    return BaseSchemaArray(listStruct, template, false)
  }
  
}


object JsonSchemaArray {
  def apply(in: JArray): JsonSchemaArray = {
    var list: List[JsonSchemaElement] = List.apply()

    in.arr.foreach(i => {
      val elementResult: JsonSchemaElement = JsonSchemaElement(i)
      list = list.appended(elementResult)
    })


    return new JsonSchemaArray(list)
  }

}
