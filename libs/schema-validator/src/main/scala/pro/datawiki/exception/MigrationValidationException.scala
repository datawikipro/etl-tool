package pro.datawiki.exception

/**
 * Исключение для ошибок валидации в миграции
 */
class MigrationValidationException(field: String, value: String, reason: String = "")
  extends MigrationException(s"Validation error for field '$field' with value '$value'${if (reason.nonEmpty) s": $reason" else ""}")
