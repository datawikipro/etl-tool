package pro.datawiki.datawarehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pro.datawiki.exception.{NotImplementedException, UnsupportedOperationException}
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class DataFrameLazySparkSql(sql: String, inInitTables: mutable.Map[String, DataFrameTrait]) extends DataFrameTrait {
  var localDf: DataFrame = null

  override def getDataFrame: DataFrame = {
    inInitTables.foreach(i => SparkContext.saveDf(i._1, i._2))
    if localDf == null then {
      localDf = SparkObject.spark.sql(sql)
      LogMode.debugDF(localDf)
    }
    return localDf
  }
  override def getFullName(in: String): String = throw NotImplementedException("getFullName not implemented for DataFrameLazySparkSql")

  override def isEmpty: Boolean = false


  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addConstantColumn(name: String, column: String): Unit = {
    localDf = getDataFrame.withColumn(name, lit(column))
  }
}
