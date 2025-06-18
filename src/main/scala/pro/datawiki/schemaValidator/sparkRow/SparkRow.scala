package pro.datawiki.schemaValidator.sparkRow

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

class SparkRow(inRow: Row, inSchema: StructType) {

  def getDataFrame: DataFrame = {
    val rowsRDD = SparkObject.spark.sparkContext.parallelize(Seq.apply(inRow))
    val df = SparkObject.spark.createDataFrame(rowsRDD, inSchema)
    LogMode.debugDF(df)
    return df
  }

}

object SparkRow {

  def getSchema(attributes: List[SparkRowAttribute]): StructType = {
    var lst: List[StructField] = List.apply()
    attributes.foreach(i => lst = lst.appended(i.getStructField))

    val sch = StructType(lst)

    return sch
  }

  def getRow(attributes: List[SparkRowAttribute]): Row = {
    var lst: List[Any] = List.apply()
    attributes.foreach(i => {
      lst = lst.appended(i.getValue)
    })
    var row: Row = Row.apply(lst *)
    return row
  }

  def apply(attributes: List[SparkRowAttribute]): SparkRow = {
    return new SparkRow(getRow(attributes), getSchema(attributes))
  }
}