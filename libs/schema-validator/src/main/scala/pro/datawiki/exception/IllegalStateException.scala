package pro.datawiki.exception

/**
 * Исключение, бросаемое при попытке выполнить операцию в неправильном состоянии объекта.
 * Используется для обозначения проблем с состоянием объекта, например,
 * когда объект не инициализирован или находится в некорректном состоянии.
 */
class IllegalStateException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Object is in illegal state", null)
}

object IllegalStateException {
  def apply(message: String, cause: Throwable): IllegalStateException = {
    new IllegalStateException(message, cause)
  }

  def apply(message: String): IllegalStateException = {
    new IllegalStateException(message)
  }
}
