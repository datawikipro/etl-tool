package pro.datawiki.sparkLoader.dictionaryEnum

enum FileStorageType {
  case json, parquet, text, avro, iceberg

  override def toString: String = {
    this.ordinal match
      case 0 => return "json"
      case 1 => return "parquet"
      case 2 => return "text"
      case 3 => return "avro"
      case 4 => return "iceberg"
      case _ => throw UnsupportedOperationException("Unsupported file storage type")
  }
}