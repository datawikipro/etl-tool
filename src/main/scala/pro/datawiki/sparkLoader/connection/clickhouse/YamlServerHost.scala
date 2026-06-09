package pro.datawiki.sparkLoader.connection.clickhouse

case class YamlServerHost(
                           host: String,
                           port: String,
                           database: String,
                           ssl: Boolean, //=[true|false]
                           sslmode: String //=[NONE|STRICT]
                         )
