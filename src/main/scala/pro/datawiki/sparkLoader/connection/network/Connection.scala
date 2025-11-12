package pro.datawiki.sparkLoader.connection.network

import java.net.{InetSocketAddress, Socket}

class Connection(host: String, port: Int) {
  val timeout: Int = 60000

  def validateHost: Boolean = {
    val socket = new Socket()
    try {
      socket.connect(new java.net.InetSocketAddress(host, port), timeout)
      return true
    } catch {
      case e: Exception =>
        return false
    } finally {
      if (socket != null) socket.close()
    }
  }
}
