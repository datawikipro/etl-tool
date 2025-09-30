package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.exception.DataProcessingException

class DataFrameDirty(partition: String, df: DataFrame, validData: Boolean) extends DataFrameTrait {
  var localDf: DataFrame = df

  override def isEmpty: Boolean = {
    if df == null then return true
    return false
  }

  override def getFullName(in: String): String = {
    return s"${in}__$partition"
  }

  override def getDataFrame: DataFrame = localDf

  override def isValidData: Boolean = validData

  override def getPartitionName: String = partition

  override def addConstantColumn(name: String, column: String): Unit = {
    localDf = localDf.withColumn(name, lit(column))
  }
}

object DataFrameDirty {
  def apply(partition: String, df: DataFrame, validData: Boolean = true): DataFrameDirty = {
    if df == null then throw DataProcessingException("DataFrame cannot be null")
    return new DataFrameDirty(partition, df, validData)
  }
}