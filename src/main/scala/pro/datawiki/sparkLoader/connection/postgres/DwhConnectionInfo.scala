package pro.datawiki.sparkLoader.connection.postgres

case class DwhConnectionInfo(
                              host: String,
                              port: Int,
                              database: String,
                              username: String,
                              password: String
                            ) {
  def hostPort: String = s"$host:$port"
}
