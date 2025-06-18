package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}


class SparkRowElementList(in: Seq[SparkRowElementRow], localtype: ArrayType) extends SparkRowElementType {
  override def getValue: Any = {
    var lst: List[Row] = List.apply()
    in.foreach(i => lst = lst.appended(i.getRow))
    return lst.toSeq
  }

  override def getType: DataType = localtype

}


object SparkRowElementList {
  def apply(in: Seq[SparkRowElementRow]): SparkRowElementList = {

    if in.isEmpty then return new SparkRowElementList(in, ArrayType(StructType(List.apply())))
    val a: SparkRowElementRow = in.head

    var subStruct: List[StructField] = List.apply()
    a.getSparkRowAttributes.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })


    return new SparkRowElementList(in, ArrayType(StructType(subStruct)))
  }
}