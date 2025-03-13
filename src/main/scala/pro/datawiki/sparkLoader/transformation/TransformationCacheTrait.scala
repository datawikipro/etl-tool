package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait

trait TransformationCacheTrait {
  def readTable: DataFrame
  def readDirty: List[DataFrameTrait]
  def saveTable(in: DataFrameTrait): Unit
}