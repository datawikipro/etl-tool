package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}

class DataFrameDirty(partition: String, df: DataFrame, validData: Boolean) extends DataFrameTrait {
  var localDf: DataFrame = df

  override def get: DataFrame = localDf

  override def isValidData: Boolean = validData

  override def getPartitionName: String = partition

  override def addColumn(name: String, column: Column): Unit = {
    localDf = localDf.withColumn(name, column)
  }
}

object DataFrameDirty {
  def apply(partition: String, df: DataFrame, validData: Boolean = true): DataFrameDirty = {
    if df == null then throw Exception()
    return new DataFrameDirty(partition, df, validData)
  }
}