package pro.datawiki.sparkLoader.connection.qdrant

import pro.datawiki.sparkLoader.connection.network.Connection

case class YamlServerHost(
                           host: String,
                           httpPort: Int,
                           grpcPort: Int
                         ) extends Connection(host, httpPort)
