package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException

trait DataFrameTrait {

  def isEmpty: Boolean

  def getDataFrame: DataFrame

  def isValidData: Boolean
  def getPartitionName: String
  def getFullName(in:String): String
  def addConstantColumn(name: String, columns: String): Unit
  
  def unionAll(add: DataFrameTrait):DataFrameTrait = throw NotImplementedException("unionAll method not implemented in base trait")
}
