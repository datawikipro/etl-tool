package pro.datawiki.schemaValidator

import org.json4s.JsonAST.{JInt, JString}
import pro.datawiki.sparkLoader.LogMode

import scala.collection.mutable

class BaseSchemaArrayTemplate(baseElement: BaseSchemaTemplate,
                              inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def getTemplate: BaseSchemaTemplate = this

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    var list: List[BaseSchemaStruct] = List.apply()

    in match
      case x: BaseSchemaArray =>
        x.getList.foreach(i => {
          list = list.appended(baseElement.extractDataFromObject(i))
        })
      case x: BaseSchemaNull =>
      case _ => throw Exception()


    return BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    throw Exception()
  }
}
