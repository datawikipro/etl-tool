package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementIntTemplate, SparkRowElementTypeTemplate}
import pro.datawiki.exception.SchemaValidationException

class BaseSchemaIntTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaInt => return BaseSchemaInt(x.getValue, x.isIgnorable)
      case _ => throw SchemaValidationException(s"Expected BaseSchemaInt, got: ${dataElement.getClass.getSimpleName}")
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case _ => throw SchemaValidationException(s"Unsupported merge with: ${in.getClass.getSimpleName}")
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementIntTemplate()
  }

  @JsonIgnore
  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("getProjectSchema not implemented in BaseSchemaIntTemplate")
  }
}
