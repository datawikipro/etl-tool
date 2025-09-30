package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.spark.sparkType.*
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStringTemplate, SparkRowElementTypeTemplate}

class BaseSchemaStringTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    if dataElement ==null then return BaseSchemaString("", inIsIgnorable)

    dataElement match
      case x: BaseSchemaString => return BaseSchemaString(x.getValue, x.isIgnorable)
      case x: BaseSchemaInt => return BaseSchemaString(x.getValue.toString(), x.isIgnorable)
      case x: BaseSchemaBoolean => return BaseSchemaString(x.getValue.toString(), x.isIgnorable)
      case x: BaseSchemaDouble => return BaseSchemaString(x.getValue.toString(), x.isIgnorable)
      case x: BaseSchemaNull => return BaseSchemaString("", x.isIgnorable)
      case other => {
        throw SchemaValidationException(s"Невозможно извлечь строковые данные из: ${other.getClass.getName}")
      }
  }

  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    in match
      case x: BaseSchemaIntTemplate => return true
      case x: BaseSchemaStringTemplate => return true
      case x: BaseSchemaBooleanTemplate => return true
      case x: BaseSchemaNullTemplate => return true
      case x: BaseSchemaDoubleTemplate => return true
      case x: BaseSchemaArrayTemplate => return false
      case null => return true
      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния со строковым шаблоном: ${other.getClass.getName}")
      }
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match {

      case x: BaseSchemaIntTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaArrayTemplate => BaseSchemaStringTemplate(inIsIgnorable) //TODO potential problem
      case null => BaseSchemaStringTemplate(inIsIgnorable)
    }
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementStringTemplate()
  }

  @JsonIgnore
  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaStringTemplate")
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaStringTemplate => return true
      case _ => return false
    }
  }
  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    data match {
      case x: BaseSchemaString => return SparkRowElementString(x.getValue)
      case x: BaseSchemaInt => return SparkRowElementInt(x.getValue)
      case x: BaseSchemaBoolean => return SparkRowElementBoolean(x.getValue)
      case x: BaseSchemaDouble => return SparkRowElementDouble(x.getValue)

      case other =>{
        throw SchemaValidationException(s"Unsupported data type for string template: ${other.getClass.getName}")
      }
    }

  }
}
