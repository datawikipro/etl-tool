package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.projectSchema.SchemaArray
import pro.datawiki.schemaValidator.spark.*
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementListTemplate, SparkRowElementStringTemplate, SparkRowElementStructTemplate, SparkRowElementTypeTemplate}

case class BaseSchemaArrayTemplate(baseElement: BaseSchemaTemplate,
                                   inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  def getBaseElement: BaseSchemaTemplate = baseElement

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    var list: List[BaseSchemaStruct] = List.apply()

    in match
      case x: BaseSchemaArray =>
        x.list.foreach(i =>
          list = list.appended(baseElement.extractDataFromObject(i)))
      case x: BaseSchemaNull => {}
      case other => throw SchemaValidationException(s"Невозможно извлечь данные массива из: ${other.getClass.getName}")


    return BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaArrayTemplate => return this.baseElement.leftValidate(x.baseElement)
      case other => {
        throw SchemaValidationException(s"Unsupported template type for array validation: ${other.getClass.getName}")
      }
    }

  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    if baseElement == null then return {
      return SparkRowElementListTemplate(baseType = SparkRowElementStringTemplate())
    }
    val base = baseElement.getSparkRowElementTemplate
    base match
      case x: SparkRowElementStringTemplate => {
        return SparkRowElementListTemplate(baseType = SparkRowElementStringTemplate())
      }
      case x: SparkRowElementStructTemplate => {
        return SparkRowElementListTemplate(baseType = x)
      }
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
  }

  override def getProjectSchema: SchemaArray = {
    baseElement match
      case x: BaseSchemaObjectTemplate => return SchemaArray(`object` = x.getProjectSchema, `type` = null)
      case x: BaseSchemaStringTemplate => return SchemaArray(`object` = null, `type` = "String")
      case x: BaseSchemaNullTemplate => return SchemaArray(`object` = null, `type` = null)
      case x: BaseSchemaIntTemplate => return SchemaArray(`object` = null, `type` = "Int")
      case null => return SchemaArray(`object` = null, `type` = null)

      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния с шаблоном массива: ${other.getClass.getName}")
      }
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaArrayTemplate => {
        if (baseElement == null) return x.getBaseElement == null
        return baseElement.equals(x.getBaseElement)
      }
      case x: BaseSchemaNullTemplate => {
        return true
      }
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
    }
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    //throw NotImplementedException("fullMerge not implemented for BaseSchemaArrayTemplate")
    in match
      case x: BaseSchemaArrayTemplate => {
        if baseElement == null then return BaseSchemaArrayTemplate(null, inIsIgnorable)
        val left = baseElement.fullMerge(x.getBaseElement)
        return BaseSchemaArrayTemplate(left, inIsIgnorable)
      }
      case x: BaseSchemaNullTemplate => {
        return BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
      }
      case x: BaseSchemaStringTemplate => {
        return BaseSchemaStringTemplate(inIsIgnorable) //TODO potential problem
      }

      case _ => {
        throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaArrayTemplate")
      }
  }

  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    baseElement match {
      case x: BaseSchemaObjectTemplate => {
        val inType: DataType = ArrayType(x.getSparkRowElementTemplate.getType)

        data match {
          case y:BaseSchemaArray =>{
            val bb = y.list.map(col=> x.getSparkRowElement(col))
            return SparkRowElement.apply(inType,bb)
          }
          case other => {
            throw SchemaValidationException(s"Unsupported data type for array element: ${other.getClass.getName}")
          }
        }


      }
      case x: BaseSchemaStringTemplate => {
        val inType: DataType = ArrayType(x.getSparkRowElementTemplate.getType)

        data match {
          case y: BaseSchemaArray => {
            return SparkRowElement.apply(inType, y.list.map(col => x.getSparkRowElement(col)))
          }
          case other => {
            throw SchemaValidationException(s"Unsupported data type for string array element: ${other.getClass.getName}")
          }
        }
      }
      case other => {
        throw SchemaValidationException(s"Unsupported base element type for array: ${other.getClass.getName}")
      }
    }


    throw NotImplementedException("getSparkRowElement not fully implemented for BaseSchemaArrayTemplate")
  }
}

object BaseSchemaArrayTemplate {
  def apply(baseElement: BaseSchemaTemplate,
            inIsIgnorable: Boolean): BaseSchemaArrayTemplate = {
    if baseElement == null then {
      return new BaseSchemaArrayTemplate(BaseSchemaNullTemplate(false), inIsIgnorable)
    }
    return new BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
  }

}