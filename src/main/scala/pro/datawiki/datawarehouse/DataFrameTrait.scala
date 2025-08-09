package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}

trait DataFrameTrait {
  
  def isEmpty: Boolean
  
  def getDataFrame: DataFrame

  def isValidData: Boolean

  def getPartitionName: String

  def addColumn(name: String, columns: Column): Unit
}
