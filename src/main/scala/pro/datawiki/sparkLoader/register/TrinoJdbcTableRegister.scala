package pro.datawiki.sparkLoader.register

import java.sql.{Connection, DriverManager, Statement}

class TrinoJdbcTableRegister(url: String, user: String) extends TableRegisterTrait {

  private def executeSql(sql: String, ignoreErrors: Boolean = false): Unit = {
    logInfo(s"Executing Trino SQL: $sql")
    var conn: Connection = null
    var stmt: Statement = null
    try {
      Class.forName("io.trino.jdbc.TrinoDriver")
      val props = new java.util.Properties()
      props.setProperty("user", user)
      conn = DriverManager.getConnection(url, props)
      stmt = conn.createStatement()
      stmt.execute(sql)
    } catch {
      case e: Exception =>
        if (ignoreErrors) {
          logWarning(s"Ignored error executing SQL ($sql): ${e.getMessage}")
        } else {
          logError(s"Failed to execute SQL: $sql", e)
          throw e
        }
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  override def registerTable(catalogName: String, schemaName: String, tableName: String, location: String): Unit = {
    logInfo(s"Registering Iceberg table in Trino via JDBC: $schemaName.$tableName")
    
    val unregisterSql =
      s"""
         |CALL $catalogName.system.unregister_table(
         |  schema_name => '$schemaName',
         |  table_name => '$tableName'
         |)
         |""".stripMargin.trim
         
    executeSql(unregisterSql, ignoreErrors = true)
    
    val registerSql =
      s"""
         |CALL $catalogName.system.register_table(
         |  schema_name => '$schemaName',
         |  table_name => '$tableName',
         |  table_location => '$location'
         |)
         |""".stripMargin.trim
         
    executeSql(registerSql)
    logInfo(s"Successfully registered table $schemaName.$tableName in Trino!")
  }

  def executeMerge(catalogName: String, schemaName: String, targetTable: String, tempTable: String, mergeKeys: List[String], columns: List[String]): Unit = {
    logInfo(s"Executing MERGE for $schemaName.$targetTable using temp table $tempTable")
    
    val joinConditions = mergeKeys.map(k => s"t.$k = s.$k").mkString(" AND ")
    val nonKeyColumns = columns.filterNot(c => mergeKeys.contains(c))
    
    val updateClause = if (nonKeyColumns.nonEmpty) {
      val updateAssignments = nonKeyColumns.map(c => s"t.$c = s.$c").mkString(", ")
      s"WHEN MATCHED THEN UPDATE SET $updateAssignments"
    } else {
      ""
    }
    
    val columnList = columns.mkString(", ")
    val valueList = columns.map(c => s"s.$c").mkString(", ")
    
    val mergeSql =
      s"""
         |MERGE INTO $catalogName.$schemaName.$targetTable t
         |USING $catalogName.$schemaName.$tempTable s
         |ON $joinConditions
         |$updateClause
         |WHEN NOT MATCHED THEN
         |  INSERT ($columnList)
         |  VALUES ($valueList)
         |""".stripMargin.trim
         
    executeSql(mergeSql)
    logInfo(s"Successfully completed MERGE for $schemaName.$targetTable!")
  }

  def dropTable(catalogName: String, schemaName: String, tableName: String): Unit = {
    logInfo(s"Dropping table in Trino: $schemaName.$tableName")
    val dropSql = s"DROP TABLE IF EXISTS $catalogName.$schemaName.$tableName"
    executeSql(dropSql, ignoreErrors = true)
  }
}
