package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementBooleanTemplate, SparkRowElementTypeTemplate}
import pro.datawiki.exception.SchemaValidationException

class BaseSchemaBooleanTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaBoolean => return BaseSchemaBoolean(x.getValue, x.isIgnorable)
      case _ => throw SchemaValidationException(s"Expected BaseSchemaBoolean, got: ${dataElement.getClass.getSimpleName}")
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaBooleanTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => return BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
      case _ => throw SchemaValidationException(s"Unsupported merge with: ${in.getClass.getSimpleName}")
  }
  
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementBooleanTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("getProjectSchema not implemented in BaseSchemaBooleanTemplate")
  }
  override def isIgnorable: Boolean = inIsIgnorable
}
