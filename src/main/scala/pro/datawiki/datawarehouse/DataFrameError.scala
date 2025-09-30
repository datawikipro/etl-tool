package pro.datawiki.datawarehouse

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}

class DataFrameError() extends DataFrameTrait {

  override def isEmpty: Boolean = true

  override def getDataFrame: DataFrame = throw UnsupportedOperationException("Cannot get DataFrame from empty DataFrame")

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addConstantColumn(name: String, column: String): Unit = throw UnsupportedOperationException("Cannot add column to empty DataFrame")

  override def getFullName(in: String): String = throw UnsupportedOperationException("Cannot get full name from empty DataFrame")
}
