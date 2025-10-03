package pro.datawiki.sparkLoader.dictionaryEnum

enum ConnectionEnum {
  case mysql, postgres, mongodb, kafka, kafkaSaslSSL, kafkaAmazon, s3Amazon, clickhouse, bigQuery,
       minioParquet, minioJson, minioJsonStream, minioText, localText, localJson, localParquet,
       selenium, jsonApi, googleAds, mail, qdrant

  override def toString: String = {
    this match {
      case `mysql` => "mysql"
      case `postgres` => "postgres"
      case `mongodb` => "mongodb"
      case `kafka` => "kafka"
      case `kafkaSaslSSL` => "kafkaSaslSSL"
      case `kafkaAmazon` => "kafkaAmazon"
      case `s3Amazon` => "s3Amazon"
      case `clickhouse` => "clickhouse"
      case `bigQuery` => "bigQuery"
      case `minioParquet` => "minioParquet"
      case `minioJson` => "minioJson"
      case `minioJsonStream` => "minioJsonStream"
      case `minioText` => "minioText"
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
      case "kafka" => ConnectionEnum.kafka
      case "kafkaSaslSSL" => ConnectionEnum.kafkaSaslSSL
      case "kafkaAmazon" => ConnectionEnum.kafkaAmazon
      case "s3Amazon" => ConnectionEnum.s3Amazon
      case "clickhouse" => ConnectionEnum.clickhouse
      case "bigQuery" => ConnectionEnum.bigQuery
      case "minioParquet" => ConnectionEnum.minioParquet
      case "minioJson" => ConnectionEnum.minioJson
      case "minioJsonStream" => ConnectionEnum.minioJsonStream
      case "minioText" => ConnectionEnum.minioText
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
