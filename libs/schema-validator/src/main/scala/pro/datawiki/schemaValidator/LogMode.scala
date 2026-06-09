package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import pro.datawiki.exception.UnsupportedOperationException

object LogMode {
  private val logger = LoggerFactory.getLogger(getClass)
  var isDebug: Boolean = false

  def setDebug(in: Boolean): Unit = {
    isDebug = in
  }

  def debugString(in: String): Boolean = {
    if (isDebug) {
      logger.info(in)
    }
    true
  }

  def getDebugFalse: Boolean = {
    if (isDebug) {
      throw new UnsupportedOperationException("getDebugFalse not implemented")
    }
    false
  }

  def debugDF(df: DataFrame, df_name: String = ""): Unit = {
    try {
      if (isDebug) {
        logger.info(s"$df_name count_rows ${df.count()}")
        df.printSchema()
        df.show(20, false)
      }
    } catch {
      case e: Exception => throw e
    }
  }
}
