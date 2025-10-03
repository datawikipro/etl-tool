package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.IllegalArgumentException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}

import java.sql.Connection

trait DatabaseTrait extends ConnectionTrait {
  def readDf(tableSchema: String, tableName: String): DataFrame

  def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame

  def getDataFrameBySQL(sql: String): DataFrame

  def runSQL(in: String): Boolean

  def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType): Unit

  def readDfSchema(tableSchema: String, tableName: String): DataFrame
  def writeDfAppend(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey:List[String]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.append, scdType = scdType)

  def writeDfOverwriteTable(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey:List[String]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.overwriteTable, scdType = scdType)

  def writeDfOverwritePartition(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey:List[String]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.overwritePartition, scdType = scdType)

  def writeDfMerge(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.merge, scdType = scdType)

  def encodeDataType(in: TableMetadataType): String

  def decodeDataType(in: String): TableMetadataType
}

object DatabaseTrait {
  def apply(connection: String, configLocation: String): DatabaseTrait = {
    ConnectionTrait.apply(null, connection, configLocation) match {
      case x: DatabaseTrait => return x
      case _ => throw IllegalArgumentException("Connection is not a DatabaseTrait")
    }
  }
}