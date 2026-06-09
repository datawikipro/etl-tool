package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementBoolean, SparkRowElementString}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementBooleanTemplate, SparkRowElementTypeTemplate}

class BaseSchemaBooleanTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {

  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaBoolean => return BaseSchemaBoolean(x.getValue, x.isIgnorable)
      case _ => throw SchemaValidationException(s"Expected BaseSchemaBoolean, got: ${dataElement.getClass.getSimpleName}")
  }

  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    in match
      case x: BaseSchemaBooleanTemplate =>  return true
      case x: BaseSchemaStringTemplate =>  return false
      case x: BaseSchemaNullTemplate =>  return true
      case _ => throw SchemaValidationException(s"Unsupported merge with: ${in.getClass.getSimpleName}")
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match {
      case x: BaseSchemaBooleanTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => return BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => return BaseSchemaBooleanTemplate(inIsIgnorable)
    }
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementBooleanTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("getProjectSchema not implemented in BaseSchemaBooleanTemplate")
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaBooleanTemplate => return true
      case _ => return false
    }
  }

  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    data match {
      case x: BaseSchemaBoolean => return SparkRowElementBoolean(x.getValue)
      case other =>{
        throw SchemaValidationException(s"Unsupported data type for boolean template: ${other.getClass.getName}")
      }
    }
  }

}
