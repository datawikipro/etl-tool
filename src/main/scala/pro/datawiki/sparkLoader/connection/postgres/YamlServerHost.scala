package pro.datawiki.sparkLoader.connection.postgres

import pro.datawiki.sparkLoader.connection.network.Connection

case class YamlServerHost(
                           host: String,
                           port: Int,
                           database: String
                         ) extends Connection(host, port)
