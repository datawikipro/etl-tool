package pro.datawiki.sparkLoader.dictionaryEnum

enum ConnectionEnum {
  case mysql, bigQuery,minioParquet,postgres

  override def toString: String = {
    this match {
      case `mysql` => "mysql"
      case `bigQuery` => "bigQuery"
      case `minioParquet`=> "minioParquet"
      case `postgres` => "postgres"
      case _ => throw Exception()
    }
  }
}
