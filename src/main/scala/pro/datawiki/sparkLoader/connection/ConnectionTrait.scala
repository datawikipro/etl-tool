package pro.datawiki.sparkLoader.connection

import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.googleAds.LoaderGoogleAds
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.kafka.kafkaBase.LoaderKafka
import pro.datawiki.sparkLoader.connection.kafka.kafkaMSK.LoaderKafkaMSK
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.local.localParquete.LoaderLocalParquet
import pro.datawiki.sparkLoader.connection.local.localText.LoaderLocalText
import pro.datawiki.sparkLoader.connection.minIo.{LoaderMinIo, LoaderMinIoStream}
import pro.datawiki.sparkLoader.connection.mysql.LoaderMySql
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.s3.LoaderS3
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium

trait ConnectionTrait {
  def close(): Unit
}

object ConnectionTrait {

  def initConnection(connection: String, configLocation: String): ConnectionTrait = {
    connection match
      case "mysql" => LoaderMySql(configLocation)
      case "postgres" => LoaderPostgres(configLocation)
      case "kafka" => LoaderKafka(configLocation)
      case "kafkaSaslSSL" => LoaderKafkaSaslSSL(configLocation)
      case "kafkaAmazon" => LoaderKafkaMSK(configLocation)
      case "s3Amazon" => LoaderS3(configLocation)
      case "clickhouse" => LoaderClickHouse(configLocation)
      case "minioParquet" => LoaderMinIo(configLocation, FileStorageType.parquet)
      case "minioJson" => LoaderMinIo(configLocation, FileStorageType.json)
      case "minioJsonStream" => LoaderMinIoStream(configLocation, FileStorageType.json)
      case "minioText" => LoaderMinIo(configLocation, FileStorageType.text)
      case "localText" => LoaderLocalText(configLocation)
      case "localJson" => LoaderLocalJson(configLocation)
      case "localParquet" => LoaderLocalParquet(configLocation)
      case "selenium" => LoaderSelenium(configLocation)
      case "jsonApi" => LoaderJsonApi(configLocation)
      case "googleAds" => LoaderGoogleAds(configLocation)
      case _ =>
        throw Exception()
  }
}