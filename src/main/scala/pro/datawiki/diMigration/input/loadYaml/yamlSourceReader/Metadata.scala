package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import pro.datawiki.exception.IllegalArgumentException
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn}
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres


object Metadata {

  def apply(metadataConnection: String,
            metadataConfigLocation: String,
            tableSchema: String,
            tableName: String): TableMetadata = {

    val metadataConnectionTrait = FileStorageTrait.apply(metadataConnection, metadataConfigLocation)
    val list = metadataConnectionTrait.readDf("metadata/tables").filter(s"table_schema = '$tableSchema'").filter(s"table_name = '$tableName'").select("columns_parsed", "primary_key_columns").collect().toList

    val columns = list.size match {
      case 0 => List.empty
      case 1 => {
        val table = list.head
        val parsedColumns: GenericRowWithSchema = table.getAs[GenericRowWithSchema]("columns_parsed")

        val b = parsedColumns.getAs[scala.collection.mutable.ArraySeq[GenericRowWithSchema]]("columns").toList
        b.filter(col => {
          if col.getAs[String]("column_name") == "ts_ms" then false else true
        }).map(col => {
          TableMetadataColumn(
            column_name = col.getAs[String]("column_name"),
            data_type = LoaderPostgres.decodeDataType(col.getAs[String]("column_type")),
            isNullable = col.getAs[String]("is_nullable") match {
              case "NO" => false
              case "YES" => true
              case fs => {
                throw IllegalArgumentException(s"Unsupported boolean value: ${fs}")
              }
            }
          )
        }).filter(col => col.column_name match {
          case "valid_from_dttm" => false
          case "valid_to_dttm" => false
          case "run_id" => false
          case _ => true
        })
      }
      case _ => {
        throw IllegalArgumentException("Unsupported metadata connection type")
      }
    }
    val primary_key_columns: List[String] = {
      if list.nonEmpty then list.map(row => {
        val primaryKeyColumns = row.getAs[String]("primary_key_columns")
        primaryKeyColumns match {
          case null => ""
          case _ => primaryKeyColumns
        }
      }).head.split(",").toList
      else List.apply()
    }
    return TableMetadata(columns = columns, primaryKey = primary_key_columns)
  }

}