package pro.datawiki.datawarehouse

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{Column, DataFrame}

trait DataFrameTrait {
  
  def getDataFrame: DataFrame

  def isValidData: Boolean

  def getPartitionName: String

  def addColumn(name: String, columns: Column): Unit
}
