package pro.datawiki.sparkLoader.dictionaryEnum

enum ConnectionEnum {
  case mysql, postgres, mongodb, kafkaBatch, kafkaStream, kafkaSaslSSLBatch, kafkaSaslSSLStream, kafkaAmazonBatch, kafkaAmazonStream, clickhouse, bigQuery,
  minioParquet, minioJson, minioJsonStream, minioText, minioAvro, minioIceberg, localText, localJson, localParquet,
  selenium, jsonApi, googleAds, mail, qdrant

  override def toString: String = {
    this match {
      case `mysql` => "mysql"
      case `postgres` => "postgres"
      case `mongodb` => "mongodb"
      case `kafkaBatch` => "kafkaBatch"
      case `kafkaStream` => "kafkaStream"
      case `kafkaSaslSSLBatch` => "kafkaSaslSSLBatch"
      case `kafkaSaslSSLStream` => "kafkaSaslSSLStream"
      case `kafkaAmazonBatch` => "kafkaAmazonBatch"
      case `kafkaAmazonStream` => "kafkaAmazonStream"
      case `clickhouse` => "clickhouse"
      case `bigQuery` => "bigQuery"
      case `minioParquet` => "minioParquet"
      case `minioJson` => "minioJson"
      case `minioJsonStream` => "minioJsonStream"
      case `minioText` => "minioText"
      case `minioAvro` => "minioAvro"
      case `minioIceberg` => "minioIceberg"
      case `localText` => "localText"
      case `localJson` => "localJson"
      case `localParquet` => "localParquet"
      case `selenium` => "selenium"
      case `jsonApi` => "jsonApi"
      case `googleAds` => "googleAds"
      case `mail` => "mail"
      case `qdrant` => "qdrant"
      case _ => throw new IllegalArgumentException(s"Unknown connection type: $this")
    }
  }

}

object ConnectionEnum {
  def fromString(connectionType: String): ConnectionEnum = {
    connectionType match {
      case "mysql" => ConnectionEnum.mysql
      case "postgres" => ConnectionEnum.postgres
      case "mongodb" => ConnectionEnum.mongodb
      case "kafkaBatch" => ConnectionEnum.kafkaBatch
      case "kafkaStream" => ConnectionEnum.kafkaStream
      case "kafkaSaslSSLBatch" => ConnectionEnum.kafkaSaslSSLBatch
      case "kafkaSaslSSLStream" => ConnectionEnum.kafkaSaslSSLStream
      case "kafkaSaslSSL" => ConnectionEnum.kafkaSaslSSLStream//TODO
      case "kafkaAmazonBatch" => ConnectionEnum.kafkaAmazonBatch
      case "kafkaAmazonStream" => ConnectionEnum.kafkaAmazonStream
      case "clickhouse" => ConnectionEnum.clickhouse
      case "bigQuery" => ConnectionEnum.bigQuery
      case "minioParquet" => ConnectionEnum.minioParquet
      case "minioJson" => ConnectionEnum.minioJson
      case "minioJsonStream" => ConnectionEnum.minioJsonStream
      case "minioText" => ConnectionEnum.minioText
      case "minioAvro" => ConnectionEnum.minioAvro
      case "minioIceberg" => ConnectionEnum.minioIceberg
      case "localText" => ConnectionEnum.localText
      case "localJson" => ConnectionEnum.localJson
      case "localParquet" => ConnectionEnum.localParquet
      case "selenium" => ConnectionEnum.selenium
      case "jsonApi" => ConnectionEnum.jsonApi
      case "googleAds" => ConnectionEnum.googleAds
      case "mail" => ConnectionEnum.mail
      case "qdrant" => ConnectionEnum.qdrant
      case _ => throw new IllegalArgumentException(s"Unknown connection type: $connectionType")
    }
  }
}
