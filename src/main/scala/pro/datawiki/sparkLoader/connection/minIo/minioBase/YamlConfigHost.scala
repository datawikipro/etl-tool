package pro.datawiki.sparkLoader.connection.minIo.minioBase

case class YamlConfigHost(
                           hostUrl: String,
                           hostName: String,
                           hostPort: Int
                         ) {
  def getUrl: String = {
    if hostUrl != null then return hostUrl
    return s"http://$hostName:$hostPort"
  }

  def getCheckUrl: String = {
    return s"$hostName:$hostPort"
  }
}