package pro.datawiki.sparkLoader.connection.minIo.minioBase

case class YamlConfig(minioHost: List[YamlConfigHost],
                      accessKey: String,
                      secretKey: String,
                      bucket: String
                     )
