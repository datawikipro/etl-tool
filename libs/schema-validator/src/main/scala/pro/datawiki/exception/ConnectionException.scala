package pro.datawiki.exception

class ConnectionException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Connection error", null)
} 