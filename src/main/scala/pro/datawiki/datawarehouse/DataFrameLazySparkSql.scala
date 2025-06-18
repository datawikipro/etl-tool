package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.task.Task

import scala.collection.mutable

class DataFrameLazySparkSql(sql: String,inInitTables: mutable.Map[String, DataFrameTrait]) extends DataFrameTrait {
  var localDf: DataFrame = null

  override def getDataFrame: DataFrame = {
    inInitTables.foreach(i=> {
      Task.saveDf(i._1, List.apply(i._2))
    })
    if localDf == null then {
      localDf = SparkObject.spark.sql(sql)
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
