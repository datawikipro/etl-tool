package pro.datawiki.sparkLoader.connection

enum FileStorageType {
  case json, parquet, text

  override def toString: String = {
    this.ordinal match
      case 0 => return "json"
      case 1 => return "parquet"
      case 2 => return "text"      
      case _ => throw Exception()
}
}