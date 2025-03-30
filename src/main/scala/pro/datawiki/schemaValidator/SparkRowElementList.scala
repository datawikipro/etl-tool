package pro.datawiki.schemaValidator

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}


class SparkRowElementList(in: Seq[SparkRowElementRow]) extends SparkRowElementType {
  override def getValue: Any = {
    var lst:List[Row] = List.apply()
    in.foreach(i=> lst = lst.appended(i.getRow))
    return lst.toSeq
  }

  override def getType: DataType = {
    if in.isEmpty then return ArrayType(StructType(List.apply()))
    val a: SparkRowElementRow = in.head

    var subStruct: List[StructField] = List.apply()
    a.getSparkRowAttributes.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })

    return ArrayType(StructType(subStruct))
  }

}
