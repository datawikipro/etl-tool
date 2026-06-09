package pro.datawiki.sparkLoader.connection.mongodb

import pro.datawiki.sparkLoader.connection.network.Connection

case class YamlServerHost(
                           host: String,
                           port: Int,
                           database: String
                         ) extends Connection(host, port)
