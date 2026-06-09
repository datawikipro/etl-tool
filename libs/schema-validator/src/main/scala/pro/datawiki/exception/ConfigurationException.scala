package pro.datawiki.exception

/**
 * Исключение, бросаемое при ошибках конфигурации.
 * Используется для обозначения проблем с настройкой системы, некорректными параметрами
 * или ошибками в конфигурационных файлах.
 */
class ConfigurationException(message: String, cause: Throwable) extends Exception(message, cause)


object ConfigurationException {
  def apply(message: String): ConfigurationException = {
    new ConfigurationException(message, null)
  }

  def apply(): ConfigurationException = {
    new ConfigurationException("Configuration error", null)
  }

  def apply(message: String, cause: Throwable): ConfigurationException = {
    new ConfigurationException(message, cause)
  }


}