package pro.datawiki.sparkLoader.connection.mongodb

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, NoSQLDatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.Connection

class LoaderMongoDb(configYaml: YamlConfig) extends ConnectionTrait, NoSQLDatabaseTrait, DataWarehouseTrait, LazyLogging {

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write
      .format("mongodb")
      .mode(writeMode.toSparkString)
      .option("uri", getMongoUri)
      .option("collection", location)
      .save()
  }

  override def readDf(location: String): DataFrame = {
    try {
      val df = SparkObject.spark.sqlContext.read
        .format("mongodb")
        .option("spark.mongodb.connection.uri", getMongoUri)
        .option("collection", location)
        //        .option("fetchsize", "10000")
        .load()

      LogMode.debugDF(df)
      return df
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def readDfSchema(location: String): DataFrame = throw Exception()

  private def getMongoUri: String = {
    if server == null then server = getServer
    return s"mongodb://${configYaml.login}:${configYaml.password}@${server.host}:${server.port}/${server.database}"
  }

  var server: YamlServerHost = null

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach(i => {
      if i.validateHost then
        return i
    })
    if configYaml.server.master.validateHost then {
      return configYaml.server.master
    }
    throw Exception()
  }
  override def close(): Unit = {

  }

}

object LoaderMongoDb extends YamlClass {
  def apply(inConfig: String): LoaderMongoDb = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderMongoDb(configYaml)
  }

  private def encodeDataType(in: TableMetadataType): String = throw Exception()

  private def decodeDataType(in: String): TableMetadataType = throw Exception()
}