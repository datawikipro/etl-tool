package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaArray, BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStringTemplate, SparkRowElementStructTemplate, SparkRowElementTypeTemplate}
import pro.datawiki.schemaValidator.spark.{SparkRowAttribute, SparkRowElementRowTemplate}

import scala.language.postfixOps


class SparkRowElementList(in: List[SparkRowElementRow], localType: ArrayType) extends SparkRowElement {
  override def getValue: List[Row] = {
    return in.map(i => i.getRow)
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


object SparkRowElementList {

  def validate(in: List[SparkRowElement]): SparkRowElementTypeTemplate = {
    var baseRow: BaseSchemaTemplate = in.head.getBaseSchemaStruct.getTemplate
    in.foreach(i => baseRow = baseRow.fullMerge(i.getBaseSchemaStruct.getTemplate))
    return baseRow.getSparkRowElementTemplate

  }

  def convertType(col: SparkRowElement): SparkRowElementRow = {
    col match {
      case x: SparkRowElementRow => return x
      case x: SparkRowElementStruct => return SparkRowElementRow(x.in)
      case _ => {
        throw IllegalArgumentException(s"Unsupported element type for list conversion: ${col.getClass.getName}")
      }
    }
  }


  def apply(in: List[SparkRowElement]): SparkRowElementList = {
    if in.isEmpty then return new SparkRowElementList(List.apply(), ArrayType(StructType(List.apply())))

    val head: SparkRowElementTypeTemplate = SparkRowElementList.validate(in)
    
    head match {
      case x: SparkRowElementRowTemplate => {
        val list1: List[SparkRowElementRow] = in.map(col => convertType(col))
        val tp: List[StructField] = x.in.map(i => StructField(name = i._1, dataType = i._2.getType, nullable = true, metadata = null))

        return new SparkRowElementList(list1, ArrayType(StructType(tp)))

      }
      case x: SparkRowElementStructTemplate => {
        val list1: List[SparkRowElementRow] = in.map(col => convertType(col))
        val tp: List[StructField] = x.in.map(i => StructField(name = i._1, dataType = i._2.getType, nullable = true, metadata = null))

        return new SparkRowElementList(list1, ArrayType(StructType(tp)))

      }
      case x: SparkRowElementStringTemplate => {
        val list1: List[SparkRowElementRow] = in.map(col => SparkRowElementRow(SparkRowAttribute(name = "value", value = col)))
        return new SparkRowElementList(list1, ArrayType(StructType(List.apply(StructField(name = "value", dataType = x.getType, nullable = true, metadata = null)))))
      }
      case _ => {
        throw NotImplementedException(s"Unsupported template type for list creation: ${head.getClass.getName}")
      }
    }
  }
}