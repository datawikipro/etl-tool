package pro.datawiki.exception

/**
 * Исключение для ошибок файловых операций
 */
class MigrationFileException(path: String, operation: String, cause: Throwable = null)
  extends MigrationException(s"File operation '$operation' failed for path: $path", cause)
