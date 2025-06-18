package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField, StructType}


class SparkRowElementStruct(in: List[SparkRowAttribute]) extends SparkRowElementType {
  override def getValue: Any = {
    var lst: List[Any] = List.apply()
    in.foreach(i => lst = lst.appended(i.getValue))

    return Row(lst *)
  }

  override def getType: DataType = {
    var subStruct: List[StructField] = List.apply()
    in.foreach(i => {
      subStruct = subStruct :+ i.getStructField
    })

    return StructType(subStruct.toSeq)
  }

  def getRow: SparkRowElementRow = {
    var list:List[ SparkRowAttribute] = List.apply()
    in.foreach(i=> {
      list = list.appended(i)
    })
    return SparkRowElementRow(list)
  }

}
