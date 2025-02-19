package pro.datawiki.sparkLoader.connection.minIo.minioParquet
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileSystemTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}

class LoaderMinIoParquet(configYaml: YamlConfig) extends LoaderMinIo(configYaml), DataWarehouseTrait, FileSystemTrait{

  @Override
  def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.
      spark.
      read.
      parquet(s"s3a://${configYaml.bucket}/$location/$segmentName/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  @Override
  def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.
      spark.
      read.
      parquet(s"s3a://${configYaml.bucket}/$location/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  @Override
  def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).parquet(s"s3a://${configYaml.bucket}/${location.replace(".", "/")}/")
  }

  @Override
  def writeDf(location: String, df: DataFrame, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()

  @Override
  def getSegments(location: String): List[String] = {
    throw Exception()
  }
}
