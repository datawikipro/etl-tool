package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.Row

class SparkRowElementList(in: Seq[SparkRowElementRow]) extends SparkRowElementType {
  override def getValue: Any = {
    var lst:List[Row] = List.apply()
    in.foreach(i=> lst = lst.appended(i.getRow))
    return lst.toSeq
  }

  override def getType: DataType = {
    val a: SparkRowElementRow = in.head

    var subStruct: List[StructField] = List.apply()
    a.getSparkRowAttributes.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })

    return ArrayType(StructType(subStruct))
  }

}
