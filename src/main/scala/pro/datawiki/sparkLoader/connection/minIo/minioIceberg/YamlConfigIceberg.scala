package pro.datawiki.sparkLoader.connection.minIo.minioIceberg

import pro.datawiki.sparkLoader.connection.minIo.minioBase.YamlConfigHost

case class YamlConfigIceberg(
                              minioHost: List[YamlConfigHost],
                              accessKey: String,
                              secretKey: String,
                              bucket: String,
                              // Iceberg catalog settings
                              warehouse: String,              // e.g. "s3a://bi-dev/warehouse"
                              hiveMetastoreUri: String,       // e.g. "thrift://hive-metastore:9083"
                              catalog: String = "iceberg",   // catalog name in spark.sql.catalog.*
                              // S3A connection settings
                              pathStyleAccess: Option[Boolean] = None,
                              establishTimeout: Option[String] = None,
                              connectionTimeout: Option[String] = None,
                              apiCallTimeout: Option[String] = None,
                              requestTimeout: Option[String] = None,
                              fastUpload: Option[String] = None,
                              fastUploadBuffer: Option[String] = None,
                              sslEnabled: Option[Boolean] = None,
                              sslChannelMode: Option[String] = None,
                              disableCertChecking: Option[Boolean] = None,
                              region: Option[String] = None,
                              catalogType: Option[String] = None,
                              register: Option[YamlConfigRegister] = None,
                              idmapSchema: Option[String] = None,
                            )
