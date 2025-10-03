package pro.datawiki.sparkLoader.connection

import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.connection.bigquery.LoaderBigQuery
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.googleAds.LoaderGoogleAds
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.kafka.kafkaBase.LoaderKafka
import pro.datawiki.sparkLoader.connection.kafka.kafkaMSK.LoaderKafkaMSK
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.local.localParquete.LoaderLocalParquet
import pro.datawiki.sparkLoader.connection.local.localText.LoaderLocalText
import pro.datawiki.sparkLoader.connection.mail.LoaderMail
import pro.datawiki.sparkLoader.connection.minIo.{LoaderMinIo, LoaderMinIoStream}
import pro.datawiki.sparkLoader.connection.mongodb.LoaderMongoDb
import pro.datawiki.sparkLoader.connection.mysql.LoaderMySql
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.qdrant.LoaderQdrant
import pro.datawiki.sparkLoader.connection.s3.LoaderS3
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, FileStorageType}
import pro.datawiki.sparkLoader.traits.LoggingTrait

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

trait ConnectionTrait extends LoggingTrait {
  def close(): Unit
  
  def getConnectionEnum(): ConnectionEnum
  
  def getCacheKey(): String = {
    ConnectionTrait.generateCacheKey(getConnectionEnum().toString, getConfigLocation())
  }
  
  def getConfigLocation(): String
}

object ConnectionTrait extends LoggingTrait {
  // Connection cache using ConcurrentHashMap for thread safety
  private val connectionCache: ConcurrentHashMap[String, ConnectionTrait] = new ConcurrentHashMap[String, ConnectionTrait]()

  def generateCacheKey(connection: String, configLocation: String): String = {
    s"${connection}:${configLocation}"
  }

  private def getCachedConnection(cacheKey: String): Option[ConnectionTrait] = {
    Option(connectionCache.get(cacheKey))
  }

  private def cacheConnection(cacheKey: String, connection: ConnectionTrait): Unit = {
    connectionCache.put(cacheKey, connection)
  }

  def removeFromCache(cacheKey: String): Unit = {
    connectionCache.remove(cacheKey)
  }

  def clearCache(): Unit = {
    connectionCache.clear()
  }

  def getCacheSize(): Int = {
    connectionCache.size()
  }

  def getCachedKeys(): java.util.Set[String] = {
    connectionCache.keySet()
  }

  def closeAndRemoveFromCache(connection: ConnectionTrait): Unit = {
    try {
      connection.close()
    } finally {
      removeFromCache(connection.getCacheKey())
    }
  }

  def apply(sourceName: String, connection: ConnectionEnum, configLocation: String): ConnectionTrait = {
    val startTime = logOperationStart("create connection", s"source: $sourceName, type: $connection, config: $configLocation")
    val cacheKey = generateCacheKey(connection.toString, configLocation)

    try {
      // Check cache first
      getCachedConnection(cacheKey) match {
        case Some(cachedConnection) =>
          logInfo(s"Using cached connection of type: $connection for source: $sourceName")
          logOperationEnd("create connection (cached)", startTime, s"type: $connection, source: $sourceName")
          return cachedConnection
        case None => // Continue with creating new connection
      }

      logInfo(s"Creating new connection of type: $connection for source: $sourceName")
      
      val locConnection = connection match
        case ConnectionEnum.mysql => LoaderMySql(configLocation)
        case ConnectionEnum.postgres => LoaderPostgres(configLocation)
        case ConnectionEnum.mongodb => LoaderMongoDb(configLocation)
        case ConnectionEnum.kafka => LoaderKafka(configLocation)
        case ConnectionEnum.kafkaSaslSSL => LoaderKafkaSaslSSL(configLocation)
        case ConnectionEnum.kafkaAmazon => LoaderKafkaMSK(configLocation)
        case ConnectionEnum.s3Amazon => LoaderS3(configLocation)
        case ConnectionEnum.clickhouse => LoaderClickHouse(sourceName, configLocation)
        case ConnectionEnum.bigQuery => LoaderBigQuery(configLocation)
        case ConnectionEnum.minioParquet => LoaderMinIo(configLocation, FileStorageType.parquet)
        case ConnectionEnum.minioJson => LoaderMinIo(configLocation, FileStorageType.json)
        case ConnectionEnum.minioJsonStream => LoaderMinIoStream(configLocation, FileStorageType.json)
        case ConnectionEnum.minioText => LoaderMinIo(configLocation, FileStorageType.text)
        case ConnectionEnum.localText => LoaderLocalText(configLocation)
        case ConnectionEnum.localJson => LoaderLocalJson(configLocation)
        case ConnectionEnum.localParquet => LoaderLocalParquet(configLocation)
        case ConnectionEnum.selenium => LoaderSelenium(configLocation)
        case ConnectionEnum.jsonApi => LoaderJsonApi(configLocation)
        case ConnectionEnum.googleAds => LoaderGoogleAds(configLocation)
        case ConnectionEnum.mail => LoaderMail(configLocation)
        case ConnectionEnum.qdrant => LoaderQdrant(configLocation)

      // Cache the newly created connection
      cacheConnection(cacheKey, locConnection)
      logInfo(s"Cached connection of type: $connection for source: $sourceName")

      logOperationEnd("create connection", startTime, s"type: $locConnection, source: $sourceName")
      return locConnection

    } catch {
      case e: Exception =>
        logError("connection creation", e, s"type: $connection, source: $sourceName")
        throw e
    }
  }

  def apply(sourceName: String, connection: String, configLocation: String): ConnectionTrait = {
    try {
      val connectionEnum = ConnectionEnum.fromString(connection)
      apply(sourceName, connectionEnum, configLocation)
    } catch {
      case e: IllegalArgumentException =>
        logError("connection creation", ConfigurationException(s"Неизвестный тип соединения: '$connection'. Пожалуйста, проверьте конфигурацию."))
        throw ConfigurationException(s"Неизвестный тип соединения: '$connection'. Пожалуйста, проверьте конфигурацию.")
    }
  }
}