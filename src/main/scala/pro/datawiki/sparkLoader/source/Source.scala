package pro.datawiki.sparkLoader.source

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.{YamlConfigSourceDBTable, YamlConfigSourceKafkaTopic}
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSource, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait, QueryTrait}

object Source {

  def run(sourceName: String,
          objectName: String,
          source: YamlConfigSourceTrait): Unit = {
    val df: DataFrame = source.getDataFrame(sourceName = sourceName)
    df.printSchema()
    df.show()
    df.createTempView(objectName)
  }

  def run(i: List[YamlConfigSource]): Unit = {
    i.foreach(i => run(sourceName = i.getSourceName, objectName = i.getObjectName, source = i.getSource))
  }
}
