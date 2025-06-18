package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementBooleanTemplate, SparkRowElementTypeTemplate}

class BaseSchemaBooleanTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaBoolean => return BaseSchemaBoolean(x.getValue, x.isIgnorable)
      case _ => throw Exception()
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaBooleanTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => return BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)

      case _ => throw Exception()
  }
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementBooleanTemplate()
  }

  override def getProjectSchema: SchemaTrait = {throw Exception()}
}
