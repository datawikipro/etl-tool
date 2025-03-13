package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame

class DataFrameOriginal(df:DataFrame) extends DataFrameTrait{
  override def get: DataFrame = df

  override def getPartitionName: String = null
}
