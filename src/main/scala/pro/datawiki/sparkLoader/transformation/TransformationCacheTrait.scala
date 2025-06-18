package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.WriteMode

trait TransformationCacheTrait {
  def readBaseTable(): DataFrame

  def readDirty(): List[DataFrameTrait]
  def deleteTable(): Unit
  def saveTable(in: DataFrameTrait, mode:WriteMode): Unit

  def append(in: TransformationCacheTrait): Boolean

  def createNewCache: TransformationCacheTrait
  
  def close():Unit
}