package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementDoubleTemplate, SparkRowElementTypeTemplate}


class BaseSchemaDoubleTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaDouble => return BaseSchemaDouble(x.getValue, x.isIgnorable)

      case _ => throw Exception()
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case _ => BaseSchemaStringTemplate(inIsIgnorable) //TODO
      case _ => throw Exception()
  }
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementDoubleTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw Exception()
  }
}
