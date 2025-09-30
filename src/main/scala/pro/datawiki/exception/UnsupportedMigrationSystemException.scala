package pro.datawiki.exception

/**
 * Исключение для ошибок неподдерживаемых систем
 */
class UnsupportedMigrationSystemException(system: String) extends MigrationException(s"Unsupported system type: $system")
