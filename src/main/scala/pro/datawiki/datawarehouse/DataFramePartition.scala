package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable


class DataFramePartition(df: mutable.Map[String,DataFrameTrait] = mutable.Map()) extends DataFrameTrait {
  
  def getPartitions:mutable.Map[String,DataFrameTrait] = df

  override def getDataFrame: DataFrame = {
    var newDf:DataFrame = null
    throw Exception()
    df.foreach(i=> {
      if newDf == null then {
        newDf = i._2.getDataFrame
      } else {
        newDf = newDf.unionAll(i._2.getDataFrame)
      }
      
    })
    return newDf
  }

  override def isValidData: Boolean = true

  override def getPartitionName: String = null

  override def addColumn(name: String, column: Column): Unit = {
    throw Exception()
//    localDf = localDf.withColumn(name, column)
  }
}
