package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementString}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStringTemplate, SparkRowElementTypeTemplate}

class BaseSchemaNullTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    return BaseSchemaNull(inIsIgnorable)
  }

  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    throw NotImplementedException("leftValidate not implemented for BaseSchemaNullTemplate")
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    throw NotImplementedException("fullMerge not implemented for BaseSchemaNullTemplate")
  }
  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementStringTemplate()
  }

  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("Метод getProjectSchema не реализован для BaseSchemaNullTemplate")
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaNullTemplate => return true
      case fs => {
        throw SchemaValidationException(s"Unsupported type for null template comparison: ${fs.getClass.getName}")
      }
    }

  }
  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    throw NotImplementedException("getSparkRowElement not implemented for BaseSchemaNullTemplate")
  }
}

object BaseSchemaNullTemplate {
  def apply(inIsIgnorable:Boolean): BaseSchemaNullTemplate= {
    return new BaseSchemaNullTemplate(inIsIgnorable)
  }
}
