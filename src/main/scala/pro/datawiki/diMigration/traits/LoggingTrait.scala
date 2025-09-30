package pro.datawiki.diMigration.traits

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

/**
 * Trait для логирования в модуле diMigration
 * Предоставляет единообразный доступ к логгеру для всех классов
 */
trait LoggingTrait {
  protected val logger = LoggerFactory.getLogger(getClass)

  /**
   * Логирует начало операции с временной меткой
   */
  protected def logOperationStart(operation: String, details: String = ""): Long = {
    val timestamp = System.currentTimeMillis()
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.info(s"Starting $message")
    timestamp
  }

  /**
   * Логирует завершение операции с измерением времени
   */
  protected def logOperationEnd(operation: String, startTime: Long, details: String = ""): Unit = {
    val duration = System.currentTimeMillis() - startTime
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.info(s"Completed $message in ${duration}ms")
  }

  /**
   * Логирует ошибку с контекстом
   */
  protected def logError(operation: String, error: Throwable, details: String = ""): Unit = {
    val message = if (details.nonEmpty) s"$operation: $details" else operation
    logger.error(s"Failed $message", error)
  }

  /**
   * Логирует предупреждение
   */
  protected def logWarning(message: String): Unit = {
    logger.warn(message)
  }

  /**
   * Логирует информационное сообщение
   */
  protected def logInfo(message: String): Unit = {
    logger.info(message)
  }

  /**
   * Устанавливает уровень логирования программно
   */
  protected def setLogLevel(level: Level): Unit = {
    logger match {
      case logbackLogger: Logger => logbackLogger.setLevel(level)
      case _ => // Не logback логгер, игнорируем
    }
  }
}
