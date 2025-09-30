package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.connection.DatabaseTrait

class DataFrameLazyDatabase(x: DatabaseTrait, sql: String) extends DataFrameTrait {
  var localDf: DataFrame = null

  override def isEmpty: Boolean = false

  override def getDataFrame: DataFrame = {
    if localDf == null then {
      localDf = x.getDataFrameBySQL(sql)
      LogMode.debugDF(localDf)
    }
    return localDf
  }

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addConstantColumn(name: String, column: String): Unit = {
    localDf = getDataFrame.withColumn(name, lit(column))
  }
  override def getFullName(in: String): String = throw NotImplementedException("getFullName not implemented for DataFrameLazyDatabase")
}
