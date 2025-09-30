package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.*
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.util.Random

trait TransformationCache extends LoggingTrait {
  def readBaseTable(connect: ConnectionTrait): DataFrame

  def readDirty (connection:ConnectionTrait): List[DataFrameTrait]

  def deleteTable(connection:ConnectionTrait): Unit

  def saveTable(in: DataFrameTrait, mode: WriteMode, connection:ConnectionTrait): Unit
  
  def close(): Unit
}