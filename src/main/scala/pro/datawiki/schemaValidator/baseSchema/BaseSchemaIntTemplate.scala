package pro.datawiki.schemaValidator.baseSchema

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementInt}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementIntTemplate, SparkRowElementTypeTemplate}

class BaseSchemaIntTemplate(inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  override def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct = {
    dataElement match
      case x: BaseSchemaInt => return BaseSchemaInt(x.getValue, x.isIgnorable)
      case _ => throw SchemaValidationException(s"Expected BaseSchemaInt, got: ${dataElement.getClass.getSimpleName}")
  }

  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    in match
      case x: BaseSchemaIntTemplate => return true
      case x: BaseSchemaStringTemplate => return false
      case x: BaseSchemaDoubleTemplate => return false
      case x: BaseSchemaNullTemplate => return true
      case _ => throw SchemaValidationException(s"Unsupported merge with: ${in.getClass.getSimpleName}")
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    in match
      case x: BaseSchemaIntTemplate => BaseSchemaIntTemplate(inIsIgnorable)
      case x: BaseSchemaStringTemplate => BaseSchemaStringTemplate(inIsIgnorable)
      case x: BaseSchemaDoubleTemplate => BaseSchemaDoubleTemplate(inIsIgnorable)
      case x: BaseSchemaNullTemplate => BaseSchemaIntTemplate(inIsIgnorable)
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    return SparkRowElementIntTemplate()
  }

  @JsonIgnore
  override def getProjectSchema: SchemaTrait = {
    throw SchemaValidationException("getProjectSchema not implemented in BaseSchemaIntTemplate")
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaIntTemplate => return true
      case _ => {
        throw SchemaValidationException(s"Unsupported type for int template comparison: ${in.getClass.getName}")
      }
    }
  }

  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElement = {
    data match {
      case x: BaseSchemaInt => return SparkRowElementInt(x.getValue)
      case other => {
        throw SchemaValidationException(s"Unsupported data type for int template: ${other.getClass.getName}")
      }
    }
  }
}
