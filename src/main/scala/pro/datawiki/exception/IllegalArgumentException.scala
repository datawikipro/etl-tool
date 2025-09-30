package pro.datawiki.exception

/**
 * Исключение, бросаемое при передаче некорректного аргумента в метод.
 * Используется для обозначения проблем с неправильными значениями параметров,
 * например, неподдерживаемые значения enum или некорректные строковые значения.
 */
class IllegalArgumentException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Illegal argument provided", null)
}

object IllegalArgumentException {
  def apply(message: String, cause: Throwable): IllegalArgumentException = {
    new IllegalArgumentException(message, cause)
  }

  def apply(message: String): IllegalArgumentException = {
    new IllegalArgumentException(message)
  }
}
