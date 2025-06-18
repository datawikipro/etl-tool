package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.types.DataType
import pro.datawiki.schemaValidator.projectSchema.SchemaTrait
import pro.datawiki.schemaValidator.sparkRow.SparkRowElementTypeTemplate

trait BaseSchemaTemplate {
  def extractDataFromObject(dataElement: BaseSchemaStruct): BaseSchemaStruct

  def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate

  def rightMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = in.leftMerge(this)

  def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = this.rightMerge(this.leftMerge(in))//TODO переделать

  def equals(in: BaseSchemaTemplate): BaseSchemaTemplate = throw Exception()

  def getSparkRowElementTemplate: SparkRowElementTypeTemplate
  
  def getProjectSchema: SchemaTrait
}

