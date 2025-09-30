package pro.datawiki.exception

class UnsupportedOperationException(message: String, cause: Throwable) extends Exception(message, cause)

object UnsupportedOperationException {
  def apply(message: String) = new UnsupportedOperationException(message, null)

  def apply(message: String, cause: Throwable) = new UnsupportedOperationException(message, cause)
}