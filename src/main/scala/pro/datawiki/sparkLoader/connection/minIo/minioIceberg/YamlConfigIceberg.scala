package pro.datawiki.sparkLoader.connection.minIo.minioIceberg

import pro.datawiki.sparkLoader.connection.minIo.minioBase.YamlConfigHost
import pro.datawiki.sparkLoader.register.YamlConfigRegister

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
                              pathStyleAccess: Option[Boolean],
                              establishTimeout: Option[String],
                              connectionTimeout: Option[String],
                              sslEnabled: Option[Boolean],
                              region: Option[String] = None,
                              catalogType: Option[String] = None,
                              register: Option[YamlConfigRegister] = None,
                            )
