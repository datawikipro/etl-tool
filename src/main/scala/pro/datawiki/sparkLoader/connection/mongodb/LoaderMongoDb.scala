package pro.datawiki.sparkLoader.connection.mongodb

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, NoSQLDatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.Connection

class LoaderMongoDb(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, NoSQLDatabaseTrait, LoggingTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating MongoDB connection")

  override def writeDf(df: DataFrame, tableFullName: String, writeMode: WriteMode): Unit = {
    val startTime = logOperationStart("write DataFrame to MongoDB", s"collection: $tableFullName, mode: $writeMode")

    try {
      logInfo(s"Writing DataFrame to MongoDB collection: $tableFullName")

      df.write
        .format("mongodb")
        .mode(writeMode.toSparkString)
        .option("uri", getMongoUri)
        .option("collection", tableFullName)
        .save()

      logOperationEnd("write DataFrame to MongoDB", startTime, s"collection: $tableFullName")

    } catch {
      case e: Exception =>
        logError("write DataFrame to MongoDB", e, s"collection: $tableFullName")
        throw e
    }
  }

  override def readDf(location: String): DataFrame = {
    val startTime = logOperationStart("read DataFrame from MongoDB", s"collection: $location")

    try {
      logInfo(s"Reading DataFrame from MongoDB collection: $location")

      var df = SparkObject.spark.sqlContext.read
        .format("mongodb")
        .option("spark.mongodb.connection.uri", getMongoUriWithOptions)
        .option("collection", location)
        .option("spark.mongodb.read.batchSize", "50000")
        .option("spark.mongodb.read.maxBatchSize", "50000")
        .option("spark.mongodb.read.maxPartitionBytes", s"${32*1024*1024}") // 32MB for better partitioning
        .option("spark.mongodb.read.maxTimeMS", "600000") // 10 minutes max operation time
        .option("spark.mongodb.read.cursorTimeoutMS", "0") // Disable cursor timeout
        .option("spark.mongodb.read.noCursorTimeout", "true") // Prevent cursor timeout
        .option("spark.mongodb.read.readPreference", "secondaryPreferred") // Use secondary for reads
        .option("spark.mongodb.read.maxAwaitTimeMS", "30000") // 30 seconds max await time
        .option("spark.mongodb.read.partitionerOptions.partitionSizeMB", "32") // 32MB partition size
        .load()

      df.write.mode("overwrite").json("/tmp/monga/")
      df = SparkObject.spark.read.json("/tmp/monga/")
      LogMode.debugDF(df)
      logOperationEnd("read DataFrame from MongoDB", startTime, s"collection: $location")
      return df

    } catch {
      case e: Exception =>
        logError("read DataFrame from MongoDB", e, s"collection: $location")
        throw e
    }
  }

//  override def readDfSchema(location: String): DataFrame = throw NotImplementedException("Method not implemented for MongoDB")

  private def getMongoUri: String = {
    if server == null then server = getServer
    return s"mongodb://${configYaml.login}:${configYaml.password}@${server.host}:${server.port}/${server.database}"
  }

  private def getMongoUriWithOptions: String = {
    if server == null then server = getServer
    val baseUri = s"mongodb://${configYaml.login}:${configYaml.password}@${server.host}:${server.port}/${server.database}"
    // Add valid connection-level timeout and retry options
    val options = Seq(
      "maxIdleTimeMS=300000", // 5 minutes
      "maxPoolSize=100",
      "minPoolSize=10",
      "maxConnecting=10",
      "serverSelectionTimeoutMS=30000", // 30 seconds
      "connectTimeoutMS=30000", // 30 seconds
      "socketTimeoutMS=300000", // 5 minutes
      "heartbeatFrequencyMS=10000", // 10 seconds
      "retryReads=true",
      "retryWrites=true",
      "readPreference=secondaryPreferred",
      "waitQueueTimeoutMS=30000" // 30 seconds wait queue timeout
    ).mkString("&")

    return s"$baseUri?$options"
  }

  var server: YamlServerHost = null

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach(i => {
      if i.validateHost then
        return i
    })
    if configYaml.server.master.validateHost then {
      return configYaml.server.master
    }
    throw NotImplementedException("Method not implemented for MongoDB")
  }

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.mongodb
  }

  override def getConfigLocation(): String = {
    _configLocation
  }

}

object LoaderMongoDb extends YamlClass {
  def apply(inConfig: String): LoaderMongoDb = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderMongoDb(configYaml, inConfig)
  }

  private def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented for MongoDB")

  private def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented for MongoDB")
}