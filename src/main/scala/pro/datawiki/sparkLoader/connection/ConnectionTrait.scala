package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait ConnectionTrait {
  def writeDf(location: String, df: DataFrame): Unit = {throw Exception()}
}
