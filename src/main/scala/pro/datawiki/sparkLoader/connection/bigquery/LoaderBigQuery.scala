package pro.datawiki.sparkLoader.connection.bigquery

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.Connection

class LoaderBigQuery(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, DatabaseTrait, LoggingTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating BigQuery connection")

  def readDfBigQuery(projectId: String,
                     datasetId: String,
                     tableId: String): DataFrame = {
    val str = "/tmp/asdasdasd.json"
    try {

      YamlClass.writefile(str, configYaml.credentialsJson)
      val df = SparkObject.spark.read
        .format("bigquery")
        .option("parentProject", projectId)
        .option("project", projectId)
        .option("dataset", datasetId)
        .option("table", tableId)
        .option("viewsEnabled", "true")
        .option("credentialsFile", str)
        .load()

      LogMode.debugDF(df)
      return df
    } catch {
      case e: Exception =>
        logger.error(s"Error reading BigQuery table", e)
        throw e
    } finally {
      YamlClass.deleteFile(str)
    }
  }

  override def runSQL(sql: String): Boolean = {
    throw NotImplementedException("Method not implemented")
  }

  private def convertBigQueryTypeToTableMetadataType(dataType: String): TableMetadataType = {
    dataType.toUpperCase match {
      case "BOOL" | "BOOLEAN" => return TableMetadataType.Boolean
      case "INT64" | "INTEGER" => return TableMetadataType.Bigint
      case "FLOAT64" | "FLOAT" => return TableMetadataType.DoublePrecision
      case "NUMERIC" | "DECIMAL" => return TableMetadataType.Numeric
      case "STRING" => return TableMetadataType.Text
      case "BYTES" => return TableMetadataType.Text
      case "DATE" => return TableMetadataType.Date
      case "DATETIME" => return TableMetadataType.TimestampWithoutTimeZone
      case "TIME" => return TableMetadataType.Text
      case "TIMESTAMP" => return TableMetadataType.TimestampWithTimeZone
      case _ => return TableMetadataType.Text
    }
  }

  override def close(): Unit = {
    // Spark BigQuery коннектор не требует явного закрытия соединений
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.bigQuery
  }

  override def getConfigLocation(): String = {
    _configLocation
  }

  override def getDataFrameBySQL(sql: String): DataFrame = {
    throw NotImplementedException("Method not implemented")
  }

  override def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")

  override def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType, partitionBy: List[(String,String)]): Unit = throw NotImplementedException("Method not implemented")

  override def setTemporaryTable(tableName: String, sql: String): Boolean = throw NotImplementedException("Method not implemented")
}

object LoaderBigQuery extends YamlClass {
  def apply(inConfig: String): LoaderBigQuery = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderBigQuery(configYaml, inConfig)
    return loader
  }
}