package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.{SchemaArray, SchemaElement, SchemaObject, SchemaTrait, SchemaType}
import pro.datawiki.schemaValidator.sparkRow.*

import scala.collection.mutable

class BaseSchemaArrayTemplate(baseElement: BaseSchemaTemplate,
                              inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  def getBaseElement = baseElement

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    var list: List[BaseSchemaStruct] = List.apply()

    in match
      case x: BaseSchemaArray =>
        x.getList.foreach(i => {
          list = list.appended(baseElement.extractDataFromObject(i))
        })
      case x: BaseSchemaNull => {}
      case _ => throw Exception()


    return BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    //TODO
    //throw Exception()
    in match
      case x: BaseSchemaArrayTemplate => {
        if baseElement == null then return BaseSchemaArrayTemplate(null, inIsIgnorable) 
        val left = baseElement.leftMerge(x.getBaseElement)
        return BaseSchemaArrayTemplate(left, inIsIgnorable)
      }
      case x: BaseSchemaNullTemplate => return BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
      case _ => throw Exception()
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    if baseElement == null then return {
      return SparkRowElementListTemplate(List.apply(SparkRowElementStringTemplate()).toSeq)
    }
    val base = baseElement.getSparkRowElementTemplate
    base match
      case x: SparkRowElementStringTemplate => {
        return SparkRowElementListTemplate(List.apply(SparkRowElementStringTemplate()))
      }
      case x: SparkRowElementStructTemplate => {
        return SparkRowElementListTemplate(List.apply(x))
      }
      case _ =>
        throw Exception()
    throw Exception()

    //
  }

  override def getProjectSchema: SchemaArray = {
    baseElement match
      case x: BaseSchemaObjectTemplate => return SchemaArray(`object` = x.getProjectSchema, `type` = null)
      case x: BaseSchemaStringTemplate => return SchemaArray(`object` = null, `type` = "String")
      case null                        => return SchemaArray( `object` =  null , `type` = null)
      case _ => throw Exception()
  }
}
