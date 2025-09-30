package pro.datawiki.exception

/**
 * Исключение, бросаемое при попытке вызова нереализованного метода.
 * Используется для обозначения методов, которые еще не реализованы
 * или находятся в процессе разработки.
 */
class NotImplementedException(message: String, cause: Throwable) extends Exception(message, cause) 

object NotImplementedException {
  def apply(message: String, cause: Throwable): NotImplementedException = {
    new NotImplementedException(message, cause)
  }

  def apply(message: String): NotImplementedException = {
    new NotImplementedException(message, null)
  }
}
