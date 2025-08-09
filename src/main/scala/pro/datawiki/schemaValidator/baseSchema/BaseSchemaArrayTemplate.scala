package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaArray
import pro.datawiki.schemaValidator.sparkRow.*

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
      case other => throw SchemaValidationException(s"Невозможно извлечь данные массива из: ${other.getClass.getName}")


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
      case x: BaseSchemaNullTemplate => {
        return BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
      }
      case x: BaseSchemaStringTemplate => {
        return BaseSchemaStringTemplate(inIsIgnorable)//TODO potential problem
      }

      case _ => {
        throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaArrayTemplate")
      }
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
      case _ => {
        throw Exception()
      }
    throw Exception()

    //
  }

  override def getProjectSchema: SchemaArray = {
    baseElement match
      case x: BaseSchemaObjectTemplate => return SchemaArray(`object` = x.getProjectSchema, `type` = null)
      case x: BaseSchemaStringTemplate => return SchemaArray(`object` = null, `type` = "String")
      case x: BaseSchemaNullTemplate   => return SchemaArray(`object` = null, `type` = null)
      case x: BaseSchemaIntTemplate    => return SchemaArray(`object` = null, `type` = "Int")
      case null                        => return SchemaArray( `object` =  null , `type` = null)

      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния с шаблоном массива: ${other.getClass.getName}")
      }
  }
}
