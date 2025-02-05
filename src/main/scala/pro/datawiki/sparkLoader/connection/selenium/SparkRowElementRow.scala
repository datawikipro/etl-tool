package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

class SparkRowElementRow(in: List[SparkRowAttribute]) {

  def getRow:Row = {
    var lst2: List[Any] = List.apply()
    in.foreach(i => lst2 = lst2 :+ i.getValue)
    val tst = Row.apply(lst2: _*)
    return tst
  }

  def getSparkRowAttributes:List[SparkRowAttribute] = in
}
