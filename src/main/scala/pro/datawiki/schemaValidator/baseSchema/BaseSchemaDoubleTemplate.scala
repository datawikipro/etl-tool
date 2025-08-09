package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementDoubleTemplate, SparkRowElementTypeTemplate}


class BaseSchemaDoubleTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaDouble => return BaseSchemaDouble(x.getValue, x.isIgnorable)

      case other => throw SchemaValidationException(s"Несовместимый тип шаблона для слияния с шаблоном с плавающей точкой: ${other.getClass.getName}")
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case _ => BaseSchemaStringTemplate(inIsIgnorable) //TODO
      case other => throw SchemaValidationException(s"Невозможно извлечь данные с плавающей точкой из: ${other.getClass.getName}")
  }
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementDoubleTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaDoubleTemplate")
  }
}
