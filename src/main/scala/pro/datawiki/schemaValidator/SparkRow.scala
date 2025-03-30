package pro.datawiki.schemaValidator

import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.schemaValidator.SparkRowAttribute
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.connection.selenium.{SeleniumList, YamlConfig}

class SparkRow(attributes: List[SparkRowAttribute]) {
  def getSchema: StructType = {

      var lst: List[StructField] = List.apply()
      attributes.foreach(i=> lst = lst.appended(i.getStructField))

      val sch = StructType(lst)

      return sch
  }

  def getRow:Row = {
    var lst:List[Any] = List.apply()
    attributes.foreach(i=> {
      lst = lst.appended(i.getValue)
    })
    var row: Row =  Row.apply(lst*)
    return row
  }

  def getDataFrame: DataFrame={
    val rowsRDD = SparkObject.spark.sparkContext.parallelize(Seq.apply(getRow))
    val schema = getSchema
    val df = SparkObject.spark.createDataFrame(rowsRDD, schema)
    LogMode.debugDF(df)
    return df
  }
  
}
