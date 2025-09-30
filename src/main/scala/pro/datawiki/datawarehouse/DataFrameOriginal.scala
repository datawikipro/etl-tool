package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.exception.UnsupportedOperationException

class DataFrameOriginal(df: DataFrame) extends DataFrameTrait {
  var localDf: DataFrame = df

  override def getFullName(in: String): String = {
    return s"${in}"
  }

  override def getDataFrame: DataFrame = {
    localDf
  }

  override def isEmpty: Boolean = throw UnsupportedOperationException("isEmpty not implemented for DataFrameOriginal")
  
  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addConstantColumn(name: String, column: String): Unit = {
    localDf = localDf.withColumn(name, lit(column))
  }
}
