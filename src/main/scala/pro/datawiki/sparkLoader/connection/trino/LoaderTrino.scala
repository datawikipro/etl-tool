package pro.datawiki.sparkLoader.connection.trino

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

class LoaderTrino(val url: String, val user: String, val configLocation: String = "") extends ConnectionTrait, DatabaseTrait, LoggingTrait {

  var connection: Connection = null

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", user)
    return prop
  }

  def getConnection: Connection = {
    if connection == null then {
      Class.forName("io.trino.jdbc.TrinoDriver")
      connection = DriverManager.getConnection(url, getProperties)
    }
    return connection
  }

  override def close(): Unit = {
    if connection != null then {
      connection.close()
      ConnectionTrait.removeFromCache(getCacheKey())
    }
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.trino

  override def getConfigLocation(): String = configLocation

  private def executeWithRetry(sql: String, ignoreNonTransientErrors: Boolean): Boolean = {
    var attempt = 1
    val maxAttempts = 6
    val sleepMs = 30000L

    while (attempt <= maxAttempts) {
      var stmt: Statement = null
      try {
        val attemptStr = if (attempt > 1) s" (attempt $attempt/$maxAttempts)" else ""
        val ignoreStr = if (ignoreNonTransientErrors) " (ignoring errors)" else ""
        logInfo(s"Executing Trino SQL$ignoreStr$attemptStr: $sql")
        
        val conn = getConnection
        stmt = conn.createStatement()
        stmt.execute(sql)
        return true
      } catch {
        case e: Exception =>
          if (stmt != null) {
            try { stmt.close() } catch { case _: Exception => }
          }
          
          val msg = if (e.getMessage != null) e.getMessage.toLowerCase else ""
          val isTransient = msg.contains("no worker nodes available") || 
                            msg.contains("timeout") || 
                            msg.contains("connection refused") || 
                            msg.contains("communication link failure") ||
                            msg.contains("server refused connection") ||
                            msg.contains("failed to connect")
          
          if (isTransient && attempt < maxAttempts) {
            logWarning(s"Transient error executing SQL (attempt $attempt/$maxAttempts): ${e.getMessage}. Retrying in ${sleepMs}ms...")
            if (connection != null) {
              try { connection.close() } catch { case _: Exception => }
              connection = null
            }
            Thread.sleep(sleepMs)
            attempt += 1
          } else {
            if (ignoreNonTransientErrors) {
              logWarning(s"Ignored error executing SQL ($sql): ${e.getMessage}")
              return false
            } else {
              logError(s"Failed to execute SQL: $sql", e)
              throw e
            }
          }
      } finally {
        if (stmt != null) {
          try { stmt.close() } catch { case _: Exception => }
        }
      }
    }
    false
  }

  override def runSQL(sql: String): Boolean = {
    executeWithRetry(sql, ignoreNonTransientErrors = false)
  }
  
  def runSQLIgnoreErrors(sql: String): Boolean = {
    executeWithRetry(sql, ignoreNonTransientErrors = true)
  }

  def executeMerge(catalogName: String, schemaName: String, targetTable: String, tempTable: String, mergeKeys: List[String], columns: List[String]): Unit = {
    logInfo(s"Executing MERGE for $schemaName.$targetTable using temp table $tempTable")
    val mergeSql = TableSqlGenerate.generateMergeSql(catalogName, schemaName, targetTable, tempTable, mergeKeys, columns)
    runSQL(mergeSql)
    logInfo(s"Successfully completed MERGE for $schemaName.$targetTable!")
  }

  def registerTable(catalogName: String, schemaName: String, tableName: String, location: String): Unit = {
    logInfo(s"Registering Iceberg table in Trino via JDBC: $schemaName.$tableName")
    val unregisterSql = TableSqlGenerate.generateUnregisterTableSql(catalogName, schemaName, tableName)
    runSQLIgnoreErrors(unregisterSql)
    
    val registerSql = TableSqlGenerate.generateRegisterTableSql(catalogName, schemaName, tableName, location)
    runSQL(registerSql)
    logInfo(s"Successfully registered table $schemaName.$tableName in Trino!")
  }

  def dropTable(catalogName: String, schemaName: String, tableName: String): Unit = {
    logInfo(s"Dropping table in Trino: $schemaName.$tableName")
    val dropSql = TableSqlGenerate.generateDropTableSql(catalogName, schemaName, tableName)
    runSQLIgnoreErrors(dropSql)
  }

  // --- DatabaseTrait methods (Not implemented for Trino DataFrame specific loads yet) ---
  
  override def getDataFrameBySQL(sql: String): DataFrame = {
    throw NotImplementedException("Trino getDataFrameBySQL not implemented yet")
  }

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = {
    throw NotImplementedException("Trino readDfSchema not implemented yet")
  }

  override def setTemporaryTable(tableName: String, sql: String): Boolean = {
    throw NotImplementedException("Trino setTemporaryTable not implemented yet")
  }

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType, partitionBy: List[(String, String)]): Unit = {
    throw NotImplementedException("Trino direct DataFrame writes are not supported, use Iceberg with Trino MERGE.")
  }

  override def encodeDataType(in: TableMetadataType): String = {
    throw NotImplementedException("Trino encodeDataType not implemented yet")
  }

  override def decodeDataType(in: String): TableMetadataType = {
    throw NotImplementedException("Trino decodeDataType not implemented yet")
  }

  override def readDf(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")
  
  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw NotImplementedException("Method not implemented")
}

object LoaderTrino extends YamlClass {
  def apply(inConfig: String): LoaderTrino = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    new LoaderTrino(configYaml.url, configYaml.user, inConfig)
  }
}
