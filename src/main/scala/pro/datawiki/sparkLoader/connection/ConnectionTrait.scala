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
import pro.datawiki.sparkLoader.dictionaryEnum.FileStorageType
import pro.datawiki.sparkLoader.traits.LoggingTrait

trait ConnectionTrait extends LoggingTrait {
  def close(): Unit
}

object ConnectionTrait extends LoggingTrait {

  def apply(sourceName: String, connection: String, configLocation: String): ConnectionTrait = {
    val startTime = logOperationStart("create connection", s"source: $sourceName, type: $connection, config: $configLocation")

    try {
      logInfo(s"Creating connection of type: $connection for source: $sourceName")

      val locConnection = connection match
        case "mysql" =>
          logInfo("Creating MySQL connection")
          LoaderMySql(configLocation)
        case "postgres" =>
          logInfo("Creating PostgreSQL connection")
          LoaderPostgres(configLocation)
        case "mongodb" =>
          logInfo("Creating MongoDB connection")
          LoaderMongoDb(configLocation)
        case "kafka" =>
          logInfo("Creating Kafka connection")
          LoaderKafka(configLocation)
        case "kafkaSaslSSL" =>
          logInfo("Creating Kafka SASL SSL connection")
          LoaderKafkaSaslSSL(configLocation)
        case "kafkaAmazon" =>
          logInfo("Creating Amazon MSK connection")
          LoaderKafkaMSK(configLocation)
        case "s3Amazon" =>
          logInfo("Creating Amazon S3 connection")
          LoaderS3(configLocation)
        case "clickhouse" =>
          logInfo("Creating ClickHouse connection")
          LoaderClickHouse(sourceName, configLocation)
        case "bigquery" =>
          logInfo("Creating BigQuery connection")
          LoaderBigQuery(configLocation)
        case "minioParquet" =>
          logInfo("Creating MinIO Parquet connection")
          LoaderMinIo(configLocation, FileStorageType.parquet)
        case "minioJson" =>
          logInfo("Creating MinIO JSON connection")
          LoaderMinIo(configLocation, FileStorageType.json)
        case "minioJsonStream" =>
          logInfo("Creating MinIO JSON Stream connection")
          LoaderMinIoStream(configLocation, FileStorageType.json)
        case "minioText" =>
          logInfo("Creating MinIO Text connection")
          LoaderMinIo(configLocation, FileStorageType.text)
        case "localText" =>
          logInfo("Creating Local Text connection")
          LoaderLocalText(configLocation)
        case "localJson" =>
          logInfo("Creating Local JSON connection")
          LoaderLocalJson(configLocation)
        case "localParquet" =>
          logInfo("Creating Local Parquet connection")
          LoaderLocalParquet(configLocation)
        case "selenium" =>
          logInfo("Creating Selenium connection")
          LoaderSelenium(configLocation)
        case "jsonApi" =>
          logInfo("Creating JSON API connection")
          LoaderJsonApi(configLocation)
        case "googleAds" =>
          logInfo("Creating Google Ads connection")
          LoaderGoogleAds(configLocation)
        case "mail" =>
          logInfo("Creating Mail connection")
          LoaderMail(configLocation)
        case "qdrant" =>
          logInfo("Creating Qdrant connection")
          LoaderQdrant(configLocation)
        case _ =>
          logError("connection creation", ConfigurationException(s"Неизвестный тип соединения: '$connection'. Пожалуйста, проверьте конфигурацию."))
          throw ConfigurationException(s"Неизвестный тип соединения: '$connection'. Пожалуйста, проверьте конфигурацию.")

      logOperationEnd("create connection", startTime, s"type: $locConnection, source: $sourceName")
      return locConnection

    } catch {
      case e: Exception =>
        logError("connection creation", e, s"type: $connection, source: $sourceName")
        throw e
    }
  }
}