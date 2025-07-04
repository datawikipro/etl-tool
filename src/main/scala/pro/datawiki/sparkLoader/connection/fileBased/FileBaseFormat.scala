package pro.datawiki.sparkLoader.connection.fileBased

enum FileBaseFormat {
  case json, csv,text, parquet

  override def toString: String = {
    this.ordinal match
            case 0 => return "json"
            case 1 => return "csv"
            case 2 => return "text"
            case 3 => return "parquet"

            case _ => {
              throw Exception()
            }
  }
}
