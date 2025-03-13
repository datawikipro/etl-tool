package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame

class DataFrameDirty(partition:String, df:DataFrame) extends DataFrameTrait{
  override def get: DataFrame = df

  override def getPartitionName: String = partition
}
