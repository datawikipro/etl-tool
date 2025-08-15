package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.{SparkRowElementString, SparkRowElementStringTemplate, SparkRowElementTypeTemplate}

class BaseSchemaNullTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    return BaseSchemaNull(inIsIgnorable)
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaBooleanTemplate => BaseSchemaBooleanTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaNullTemplate(inIsIgnorable)
      case x: BaseSchemaArrayTemplate => BaseSchemaArrayTemplate(x.getBaseElement,inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaObjectTemplate => x
      case _ => {
        throw Exception()
      }
  }
  
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementStringTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaNullTemplate")
  }
  override def isIgnorable: Boolean = inIsIgnorable
}
