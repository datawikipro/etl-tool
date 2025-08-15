package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementStringTemplate, SparkRowElementTypeTemplate}

class BaseSchemaStringTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
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

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaArrayTemplate => BaseSchemaStringTemplate(inIsIgnorable)//TODO potential problem
      case null => BaseSchemaStringTemplate(inIsIgnorable)
      case other => {
        throw SchemaValidationException(s"Несовместимый тип шаблона для слияния со строковым шаблоном: ${other.getClass.getName}")
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
}
