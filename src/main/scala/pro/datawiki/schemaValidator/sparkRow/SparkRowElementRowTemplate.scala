package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.Row

class SparkRowElementRowTemplate(in: List[SparkRowAttributeTemplate]) {

  def getRow: Row = {
    var lst2: List[Any] = List.apply()
    in.foreach(i => lst2 = lst2 :+ i.getValueTemplate)
    val tst = Row.apply(lst2 *)
    return tst
  }

  def getSparkRowListAttributeTemplate: List[SparkRowAttributeTemplate] = in

}
