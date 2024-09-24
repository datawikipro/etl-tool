package pro.datawiki.sparkLoader.configuration

case class YamlConfigSource(sourceName: String,
                            objectName: String,
                            sourceDb: YamlConfigSourceDBTable,
                            sourceKafkaTopic: YamlSourceKafkaTopic
                           )