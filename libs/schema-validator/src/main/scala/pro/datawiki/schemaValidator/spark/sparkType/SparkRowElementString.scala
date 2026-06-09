package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.types.DataType
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaString, BaseSchemaStruct}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStringTemplate, SparkRowElementTypeTemplate}


class SparkRowElementString(in: String) extends SparkRowElement {
  override def getValue: String = in

  val localtype: SparkRowElementTypeTemplate = SparkRowElementStringTemplate()

  override def getType: DataType = localtype.getType

  override def getBaseSchemaStruct: BaseSchemaStruct = BaseSchemaString(in, false)
}

object SparkRowElementString {
  def apply(in: Any): SparkRowElementString = {
    if in == null then return new SparkRowElementString(null)
    in match {
      case x: String => new SparkRowElementString(x)
      case x: SparkRowElementString => x
      case fs => {
        throw IllegalArgumentException(s"Unsupported type for SparkRowElementString: ${fs.getClass.getName}")
      }
    }

  }
}