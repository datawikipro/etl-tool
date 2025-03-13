package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame

trait DataFrameTrait {
  def get: DataFrame = throw Exception()
  def getPartitionName : String 
}
