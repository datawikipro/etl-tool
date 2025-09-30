package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaArray, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStringTemplate, SparkRowElementStructTemplate, SparkRowElementTypeTemplate}
import pro.datawiki.schemaValidator.spark.{SparkRowAttribute, SparkRowElementRowTemplate}

import scala.language.postfixOps


class SparkRowElementArray(in: List[SparkRowElement], localType: DataType) extends SparkRowElement {
  override def getValue: List[Any] = {
    return in.map(i => i.getValue)
  }

  override def getType: DataType = {
    localType
  }

  override def getBaseSchemaStruct: BaseSchemaStruct = {
    return BaseSchemaArray(list = in.map(col => {
      col.getBaseSchemaStruct
    }), false)
  }
}
