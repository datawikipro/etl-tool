package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
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
      case _ => throw Exception()
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case null => BaseSchemaStringTemplate(inIsIgnorable)
      case _ => throw Exception()
  }


  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementStringTemplate()
  }

  @JsonIgnore
  override def getProjectSchema: SchemaTrait = {
    throw Exception()
  }
}
