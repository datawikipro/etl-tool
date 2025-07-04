package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.connection.DatabaseTrait

class DataFrameLazyDatabase(x: DatabaseTrait, sql: String) extends DataFrameTrait {
  var localDf: DataFrame = null

  override def isEmpty: Boolean = throw Exception()


  override def getDataFrame: DataFrame = {
    if localDf == null then {
      localDf = x.getDataFrameBySQL(sql)
      LogMode.debugDF(localDf)
    }
    return localDf
  }

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addColumn(name: String, column: Column): Unit = {
    localDf = getDataFrame.withColumn(name, column)
  }
}
