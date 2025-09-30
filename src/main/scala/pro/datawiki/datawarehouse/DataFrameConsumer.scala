package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.exception.NotImplementedException

class DataFrameConsumer(df: DataFrame) extends DataFrameTrait {
  var localDf: DataFrame = df

  override def isEmpty: Boolean = {
    if df == null then return true
    return false
  }

  override def getFullName(in: String): String = throw NotImplementedException("getFullName not implemented for DataFrameConsumer")
  override def getDataFrame: DataFrame = localDf

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addConstantColumn(name: String, column: String): Unit = {
    localDf = localDf.withColumn(name, lit(column))
  }

}
