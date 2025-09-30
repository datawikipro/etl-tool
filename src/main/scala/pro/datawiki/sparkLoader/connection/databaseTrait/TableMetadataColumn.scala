package pro.datawiki.sparkLoader.connection.databaseTrait

case class TableMetadataColumn(column_name: String,
                               data_type: TableMetadataType,
                               isNullable: Boolean
                              ) 
