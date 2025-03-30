package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}

trait DataFrameTrait {
  def get: DataFrame
  def isValidData: Boolean
  def getPartitionName : String
  def addColumn(name:String, columns: Column):Unit
}
