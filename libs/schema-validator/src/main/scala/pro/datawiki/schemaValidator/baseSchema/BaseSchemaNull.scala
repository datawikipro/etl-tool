package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElement, SparkRowElementString}

case class BaseSchemaNull(inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaNullTemplate(inIsIgnorable)
  //  override def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement = {
  //    schemaObject match
  //      case x: BaseSchemaNull => return x
  //      case x: BaseSchemaString => {
  //        if x.in == null then
  //          return BaseSchemaString("",inIsIgnorable)
  //        throw SchemaValidationException("Cannot merge null schema with non-null string schema")
  //      }
  //      case _ => throw SchemaValidationException("Unsupported schema type for merging with null schema")
  //  }

  def getSparkRowElement: SparkRowElement = SparkRowElementString("")

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
