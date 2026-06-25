package pro.datawiki.sparkLoader.connection.trino

object TableSqlGenerate {
  def generateRegisterTableSql(catalogName: String, schemaName: String, tableName: String, location: String): String = {
    s"""
       |CALL $catalogName.system.register_table(
       |  schema_name => '$schemaName',
       |  table_name => '$tableName',
       |  table_location => '$location'
       |)
       |""".stripMargin.trim
  }

  def generateUnregisterTableSql(catalogName: String, schemaName: String, tableName: String): String = {
    s"""
       |CALL $catalogName.system.unregister_table(
       |  schema_name => '$schemaName',
       |  table_name => '$tableName'
       |)
       |""".stripMargin.trim
  }

  def generateDropTableSql(catalogName: String, schemaName: String, tableName: String): String = {
    s"DROP TABLE IF EXISTS $catalogName.$schemaName.$tableName"
  }

  def generateMergeSql(catalogName: String, schemaName: String, targetTable: String, tempTable: String, mergeKeys: List[String], columns: List[String]): String = {
    val joinConditions = mergeKeys.map(k => s"t.$k = s.$k").mkString(" AND ")
    val nonKeyColumns = columns.filterNot(c => mergeKeys.contains(c))
    
    val updateClause = if (nonKeyColumns.nonEmpty) {
      val updateAssignments = nonKeyColumns.map(c => s"$c = s.$c").mkString(", ")
      s"WHEN MATCHED THEN UPDATE SET $updateAssignments"
    } else {
      ""
    }
    
    val columnList = columns.mkString(", ")
    val valueList = columns.map(c => s"s.$c").mkString(", ")
    
    s"""
       |MERGE INTO $catalogName.$schemaName.$targetTable t
       |USING $catalogName.$schemaName.$tempTable s
       |ON $joinConditions
       |$updateClause
       |WHEN NOT MATCHED THEN
       |  INSERT ($columnList)
       |  VALUES ($valueList)
       |""".stripMargin.trim
  }
}
