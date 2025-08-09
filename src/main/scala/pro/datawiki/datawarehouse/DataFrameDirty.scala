package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.exception.DataProcessingException

class DataFrameDirty(partition: String, df: DataFrame, validData: Boolean) extends DataFrameTrait {
  var localDf: DataFrame = df
  
  override def isEmpty: Boolean = {
    if df == null then return true
    return false
  }
  
  override def getDataFrame: DataFrame = localDf

  override def isValidData: Boolean = validData

  override def getPartitionName: String = partition

  override def addColumn(name: String, column: Column): Unit = {
    localDf = localDf.withColumn(name, column)
  }
}

object DataFrameDirty {
  def apply(partition: String, df: DataFrame, validData: Boolean = true): DataFrameDirty = {
    if df == null then throw new DataProcessingException("DataFrame cannot be null")
    return new DataFrameDirty(partition, df, validData)
  }
}