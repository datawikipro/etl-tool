package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.YamlConfigTarget

trait ConnectionTrait {
  def writeDf(location: YamlConfigTarget, df: DataFrame,autoInsertIdmapCCD:Boolean,columnsLogicKey:List[String]): Unit = {throw Exception()}
  def close():Unit = {}
}
