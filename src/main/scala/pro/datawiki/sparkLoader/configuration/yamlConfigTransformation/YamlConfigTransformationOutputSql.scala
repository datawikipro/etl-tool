package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

case class YamlConfigTransformationOutputSql(
                                              sourceName: String,
                                              sql: String
                                            ) extends YamlConfigTransformationTrait {
  override def getDataFrame: DataFrame = {
    val df: DataFrame = Connection.getConnection(sourceName) match
      case x: DatabaseTrait => x.getDataFrameBySQL(s"${sql}")
      case _ => throw Exception()
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

}
