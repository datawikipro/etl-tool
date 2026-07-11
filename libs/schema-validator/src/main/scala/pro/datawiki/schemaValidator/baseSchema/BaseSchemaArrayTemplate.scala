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
    return SparkRowElementListTemplate(baseType = base)
  }

  override def getProjectSchema: SchemaArray = {
    baseElement match
      case x: BaseSchemaObjectTemplate => return SchemaArray(`object` = x.getProjectSchema, `type` = null)
      case x: BaseSchemaStringTemplate => return SchemaArray(`object` = null, `type` = "String")
      case x: BaseSchemaIntTemplate => return SchemaArray(`object` = null, `type` = "Int")
      case x: BaseSchemaDoubleTemplate => return SchemaArray(`object` = null, `type` = "Double")
      case x: BaseSchemaBooleanTemplate => return SchemaArray(`object` = null, `type` = "Boolean")
      case x: BaseSchemaNullTemplate => return SchemaArray(`object` = null, `type` = null)
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
    if (baseElement == null) {
      throw SchemaValidationException("baseElement cannot be null when extracting data")
    }
    
    val inType: DataType = ArrayType(baseElement.getSparkRowElementTemplate.getType)
    
    data match {
      case y: BaseSchemaArray => {
        return SparkRowElement.apply(inType, y.list.map(col => baseElement.getSparkRowElement(col)))
      }
      case other => {
        throw SchemaValidationException(s"Unsupported data type for array element: ${other.getClass.getName}")
      }
    }
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