package pro.datawiki.sparkLoader.connection.fileBased

import pro.datawiki.exception.UnsupportedOperationException

enum FileBaseFormat {
  case `json`, `csv`, `text`, `parquet`

  override def toString: String = {
    this match
      case `json` => return "json"
      case `csv` => return "csv"
      case `text` => return "text"
      case `parquet` => return "parquet"

      case _ => {
        throw UnsupportedOperationException("Unsupported file format")
      }
  }
}
