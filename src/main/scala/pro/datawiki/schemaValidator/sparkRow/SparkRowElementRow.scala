package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row

class SparkRowElementRow(in: List[SparkRowAttribute]) {

  def getRow: Row = {
    var lst2: List[Any] = List.apply()
    in.foreach(i => lst2 = lst2 :+ i.getValue)
    val tst = Row.apply(lst2 *)
    return tst
  }

  def getSparkRowAttributes: List[SparkRowAttribute] = in

}
