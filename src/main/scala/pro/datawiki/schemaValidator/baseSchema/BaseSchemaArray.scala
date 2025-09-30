package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.spark.*
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElementList, SparkRowElementRow, SparkRowElementString, SparkRowElementStruct}

case class BaseSchemaArray(list: List[BaseSchemaStruct],
                           baseElement: BaseSchemaTemplate,
                           inIsIgnorable: Boolean) extends BaseSchemaStruct {

  def getSubElement: BaseSchemaTemplate = baseElement

  def packageDataFrame: DataFrame = {
    try {
      val in1: List[BaseSchemaObject] = list.map { col => {
        col match {
          case x: BaseSchemaObject => x
          case _ => throw SchemaValidationException("Expected BaseSchemaObject in array list")
        }
      }
      }.toList


      val in_template: BaseSchemaObjectTemplate = baseElement match {
        case x: BaseSchemaObjectTemplate => x
        case _ => {
          throw SchemaValidationException("Expected BaseSchemaObjectTemplate for array base element")
        }
      }


      return SparkConstructor.getDataFrameFromListBaseSchemaObjects(in1, in_template)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def getSparkRowElement: SparkRowElementList = {
    baseElement match {
      case y: BaseSchemaNullTemplate => {
        return SparkRowElementList(List.empty)
      }
      case y: BaseSchemaStringTemplate => {
        var l: List[SparkRowElementRow] =
          list.map(i => {
            i.getSparkRowElement match
              case x: SparkRowElementString => (SparkRowElementRow(List.apply(SparkRowAttribute(name = "data", value = x))))
              case other => {
                throw SchemaValidationException(s"Ожидался SparkRowElementString в BaseSchemaArray со строковым шаблоном, но получен: ${other.getClass.getName}")
              }
          })
        return SparkRowElementList(l)
      }

      case y: BaseSchemaObjectTemplate => {
        val l: List[SparkRowElementRow] = list.map(i => {
          i.getSparkRowElement match
            case x: SparkRowElementStruct => x.getRow
            case other => {
              throw SchemaValidationException(s"Ожидался SparkRowElementStruct в BaseSchemaArray с объектным шаблоном, но получен: ${other.getClass.getName}")
            }
        })

        return SparkRowElementList(l)
      }
      case unknown => {
        throw SchemaValidationException(s"Неподдерживаемый тип шаблона в BaseSchemaArray: ${unknown.getClass.getName}")
      }
    }

  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
}

object BaseSchemaArray {
  def apply(list: List[BaseSchemaStruct],
            baseElement: BaseSchemaTemplate,
            inIsIgnorable: Boolean): BaseSchemaArray = {

    return new BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  def apply(list: List[BaseSchemaStruct],
            inIsIgnorable: Boolean): BaseSchemaArray = {
    var varBaseSchema: BaseSchemaTemplate = list.head.getTemplate
    list.foreach(col => {
      varBaseSchema = col.getTemplate.fullMerge(varBaseSchema)
    })

    return apply(list, varBaseSchema, inIsIgnorable)
  }

}