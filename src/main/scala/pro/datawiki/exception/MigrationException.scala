package pro.datawiki.exception

/**
 * Базовое исключение для всех ошибок миграции данных
 */
class MigrationException(message: String, cause: Throwable) extends DataProcessingException(message, cause) {
  def this(message: String) = this(message, null)

  def this() = this("Migration error", null)
}

object MigrationException {
  def apply(message: String, cause: Throwable): MigrationException =
    new MigrationException(message, cause)

  def apply(message: String): MigrationException =
    new MigrationException(message, null)
}