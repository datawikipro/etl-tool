package pro.datawiki.exception

class ValidationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Validation error", null)
} 