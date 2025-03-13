package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}

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

}

object SparkRow {
  def apply(in:SeleniumList, config: YamlConfig):SparkRow = {
    if config == null then {
      throw Exception()
    }
    var listFieldsAttribute: List[SparkRowAttribute] = List.apply()

    config.getInSchema.foreach(i => {
      listFieldsAttribute = listFieldsAttribute.appended(i.getStructField(in))
    })

    return new SparkRow(listFieldsAttribute)
  }


}