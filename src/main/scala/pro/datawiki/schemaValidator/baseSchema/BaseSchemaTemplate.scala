package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.types.DataType
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.SparkRowElementTypeTemplate
import pro.datawiki.exception.SchemaValidationException

trait BaseSchemaTemplate {
  def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct

  def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate

  private def rightMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = in.leftMerge(this)

  def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = this.rightMerge(this.leftMerge(in))//TODO переделать

  def equals(in: BaseSchemaTemplate): BaseSchemaTemplate = throw SchemaValidationException("equals method not implemented in BaseSchemaTemplate")

  def getSparkRowElementTemplate: SparkRowElementTypeTemplate
  
  def getProjectSchema: SchemaTrait
  def isIgnorable: Boolean
}

object BaseSchemaTemplate {
  def apply(in: String): BaseSchemaTemplate = {

    in match
      case "String" => return BaseSchemaStringTemplate(false)
      case "Int" => return BaseSchemaIntTemplate(false)
      case "Long" => return BaseSchemaIntTemplate(false)
      case "Boolean" => return BaseSchemaBooleanTemplate(false)
      case _ => {
        throw Exception()
      }
  }
}