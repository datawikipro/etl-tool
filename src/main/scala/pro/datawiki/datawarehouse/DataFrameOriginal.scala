package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}

class DataFrameOriginal(df: DataFrame) extends DataFrameTrait {
  var localDf: DataFrame = df

  override def get: DataFrame = localDf

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addColumn(name: String, column: Column): Unit = {
    localDf = localDf.withColumn(name, column)
  }
}
