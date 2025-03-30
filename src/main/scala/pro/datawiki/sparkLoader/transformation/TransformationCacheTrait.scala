package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait

trait TransformationCacheTrait {
  def readBaseTable(): DataFrame
  def readDirty(): List[DataFrameTrait]
  def saveTable(in: DataFrameTrait): Unit
  def append(in:TransformationCacheTrait): Boolean
}