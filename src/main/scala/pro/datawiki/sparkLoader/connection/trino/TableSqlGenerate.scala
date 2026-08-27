package pro.datawiki.sparkLoader.connection.trino

import org.apache.spark.sql.types._

object TableSqlGenerate {
  def sparkTypeToTrinoSql(dataType: DataType): String = {
    dataType match {
      case ByteType | ShortType | IntegerType => "INTEGER"
      case LongType => "BIGINT"
      case FloatType | DoubleType => "DOUBLE"
      case dt: DecimalType => s"DECIMAL(${dt.precision}, ${dt.scale})"
      case StringType => "VARCHAR"
      case BooleanType => "BOOLEAN"
      case TimestampType | TimestampNTZType => "TIMESTAMP(6)"
      case DateType => "DATE"
      case BinaryType => "VARBINARY"
      case ArrayType(elementType, _) => s"ARRAY(${sparkTypeToTrinoSql(elementType)})"
      case StructType(fields) =>
        val fieldsSql = fields.map(f => s""""${f.name}" ${sparkTypeToTrinoSql(f.dataType)}""").mkString(", ")
        s"ROW($fieldsSql)"
      case _ => "VARCHAR"
    }
  }

  def generateAddColumnSql(catalogName: String, schemaName: String, tableName: String, columnName: String, trinoType: String): String = {
    s"""ALTER TABLE $catalogName.$schemaName.$tableName ADD COLUMN IF NOT EXISTS "$columnName" $trinoType"""
  }

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

  def generateMergeSql(catalogName: String, schemaName: String, targetTable: String, tempTable: String, mergeKeys: List[String], columns: List[String], targetColumns: Option[Set[String]] = None): String = {
    val validColumns = targetColumns match {
      case Some(tc) if tc.nonEmpty =>
        val lowerTarget = tc.map(_.toLowerCase)
        columns.filter(c => lowerTarget.contains(c.toLowerCase))
      case _ => columns
    }
    val joinConditions = mergeKeys.map(k => s"""t."$k" = s."$k"""").mkString(" AND ")
    val nonKeyColumns = validColumns.filterNot(c => mergeKeys.map(_.toLowerCase).contains(c.toLowerCase))
    
    val updateClause = if (nonKeyColumns.nonEmpty) {
      val updateAssignments = nonKeyColumns.map(c => s""""$c" = s."$c"""").mkString(", ")
      s"WHEN MATCHED THEN UPDATE SET $updateAssignments"
    } else {
      ""
    }
    
    val columnList = validColumns.map(c => s""""$c"""").mkString(", ")
    val valueList = validColumns.map(c => s"""s."$c"""").mkString(", ")
    
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

