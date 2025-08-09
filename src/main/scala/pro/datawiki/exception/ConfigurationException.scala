package pro.datawiki.exception

/**
   * Исключение, бросаемое при ошибках конфигурации.
   * Используется для обозначения проблем с настройкой системы, некорректными параметрами
   * или ошибками в конфигурационных файлах.
   */
class ConfigurationException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this("Configuration error", null)
} 