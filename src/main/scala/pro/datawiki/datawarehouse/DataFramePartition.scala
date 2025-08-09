package pro.datawiki.datawarehouse

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.exception.UnsupportedOperationException

import scala.collection.mutable


class DataFramePartition(df: mutable.Map[String,DataFrameTrait] = mutable.Map()) extends DataFrameTrait {
  
  def getPartitions:mutable.Map[String,DataFrameTrait] = df

  override def isEmpty: Boolean = throw new UnsupportedOperationException("isEmpty not implemented for DataFramePartition")


  override def getDataFrame: DataFrame = {
    var newDf:DataFrame = null
    throw new UnsupportedOperationException("getDataFrame not implemented for DataFramePartition")
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
    throw new UnsupportedOperationException("addColumn not implemented for DataFramePartition")
//    localDf = localDf.withColumn(name, column)
  }
}
