package pro.datawiki.sparkLoader.connection.databaseTrait

import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

case class TableMetadata(
                          columns: List[TableMetadataColumn],
                          primaryKey: List[String]
                        ) {
  def getWriteMode: WriteMode={
    columns.isEmpty match {
      case true => WriteMode.overwritePartition
      case false => WriteMode.mergeFull
    }

  }
}
