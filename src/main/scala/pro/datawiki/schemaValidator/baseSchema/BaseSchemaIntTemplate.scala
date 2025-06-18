package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementIntTemplate, SparkRowElementTypeTemplate}

class BaseSchemaIntTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaInt => return BaseSchemaInt(x.getValue, x.isIgnorable)

      case _ => throw Exception()
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case _ => throw Exception()
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementIntTemplate()
  }

  @JsonIgnore
  override def getProjectSchema: SchemaTrait = {
    throw Exception()
  }
}
