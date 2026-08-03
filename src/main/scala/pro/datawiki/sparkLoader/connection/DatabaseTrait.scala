package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.IllegalArgumentException
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}

trait DatabaseTrait extends ConnectionTrait {
  //-----------------------------------------------------------
  def readDf(tableSchema: String, tableName: String): DataFrame

  def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame

  //-----------------------------------------------------------
  def getDataFrameBySQL(sql: String): DataFrame

  /**
   * Applies dialect-specific row limit to SQL query.
   * Default implementation appends standard SQL 'LIMIT limit' if limit > 0 and no LIMIT exists.
   */
  def applyLimitToSql(sql: String, limit: Int): String = {
    if (limit <= 0) return sql
    val hasLimit = sql.matches("(?i).*\\s+limit\\s+\\d+\\s*$")
    if (hasLimit) sql else s"$sql limit $limit"
  }

  /**
   * Dialect-specific SQL expression for the SCD_2/SCD_3 "infinity" date (year 2100).
   * Must be implemented in each connector subclass to match the target database SQL dialect.
   *
   * Examples:
   *   PostgreSQL:  to_date('2100','yyyy')
   *   MSSQL:       CONVERT(date, '2100-01-01')
   *   Trino:       date '2100-01-01'
   *   Spark SQL:   date('2100-01-01')
   */
  def scdInfinityDateExpr: String

  /**
   * SQL WHERE predicate to select only active (current) SCD_2/SCD_3 records.
   * Uses the dialect-specific [[scdInfinityDateExpr]].
   */
  def scdActiveFilter: String = s"valid_to_dttm = $scdInfinityDateExpr"

  /**
   * Dialect-specific SQL expression to cast a value to string/varchar.
   * Default: standard SQL CAST(expr AS VARCHAR).
   * Override in connectors where syntax differs (e.g., Spark SQL uses STRING).
   *
   * Examples:
   *   Standard SQL / Trino / Postgres: CAST(expr AS VARCHAR)
   *   MSSQL:                           CAST(expr AS NVARCHAR(MAX))
   *   Spark SQL:                       CAST(expr AS STRING)
   */
  def castToString(expr: String): String = s"CAST($expr AS VARCHAR)"

  def runSQL(in: String): Boolean

  def readDfSchema(tableSchema: String, tableName: String): DataFrame

  //-----------------------------------------------------------
  def setTemporaryTable(tableName: String, sql: String): Boolean

  //-----------------------------------------------------------
  def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType, partitionBy: List[(String,String)]): Unit

  def writeDfAppend(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.append, scdType = scdType, partitionBy = partitionBy)

  def writeDfOverwriteTable(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.overwriteTable, scdType = scdType, partitionBy = partitionBy)

  def writeDfOverwritePartition(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.overwritePartition, scdType = scdType, partitionBy = partitionBy)

  def writeDfMergeDelta(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.mergeDelta, scdType = scdType, partitionBy = partitionBy)

  def writeDfMergeFull(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = writeDf(df = df, tableSchema = tableSchema, tableName = tableName, writeMode = WriteMode.mergeFull, scdType = scdType, partitionBy = partitionBy)

  //-----------------------------------------------------------
  def encodeDataType(in: TableMetadataType): String

  def decodeDataType(in: String): TableMetadataType

  def convertComplexTypesToJson(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.*
    import org.apache.spark.sql.types.*

    val columns = df.schema.fields.map { field =>
      field.dataType match {
        // Structs and Maps -> convert to JSON string
        case StructType(_) | MapType(_, _, _) =>
          to_json(col(field.name)).as(field.name)

        // Arrays: keep simple arrays of strings or ints as arrays; otherwise stringify
        case ArrayType(StringType, _) | ArrayType(IntegerType, _) =>
          col(field.name)
        case ArrayType(_, _) =>
          to_json(col(field.name)).as(field.name)

        // Keep simple types as-is
        case _ => col(field.name)
      }
    }

    df.select(columns *)
  }
}

object DatabaseTrait {
  def apply(connection: String, configLocation: String): DatabaseTrait = {
    ConnectionTrait.apply(null, connection, configLocation) match {
      case x: DatabaseTrait => return x
      case _ => throw IllegalArgumentException("Connection is not a DatabaseTrait")
    }
  }
}