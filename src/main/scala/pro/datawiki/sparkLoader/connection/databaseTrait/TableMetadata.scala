package pro.datawiki.sparkLoader.connection.databaseTrait

case class TableMetadata(
                          columns: List[TableMetadataColumn],
                          primaryKey: List[String]
                        )
