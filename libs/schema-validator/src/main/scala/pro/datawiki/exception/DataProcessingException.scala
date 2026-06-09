package pro.datawiki.exception

/**
 * Исключение, бросаемое при ошибках обработки данных.
 * Используется для обозначения проблем, возникающих во время обработки, трансформации
 * или анализа данных в процессе ETL-операций.
 */
class DataProcessingException(message: String, cause: Throwable) extends Exception(message, cause)

object DataProcessingException {
  def apply(message: String, cause: Throwable): DataProcessingException = {
    new DataProcessingException(message, cause: Throwable)
  }

  def apply(message: String): DataProcessingException = {
    new DataProcessingException(message, null)
  }

  def apply(): DataProcessingException = {
    new DataProcessingException("Data processing error", null)
  }
}
