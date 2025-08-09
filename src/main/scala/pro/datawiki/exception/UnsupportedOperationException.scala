package pro.datawiki.exception

class UnsupportedOperationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this("Operation not supported", null)
} 