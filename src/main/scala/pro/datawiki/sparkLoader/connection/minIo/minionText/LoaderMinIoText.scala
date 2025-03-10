package pro.datawiki.sparkLoader.connection.minIo.minionText

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileSystemTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

class LoaderMinIoText(configYaml: YamlConfig) extends LoaderMinIo(configYaml),DataWarehouseTrait, FileSystemTrait{

  @Override
  def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.
      spark.
      read.
      text(s"s3a://${configYaml.bucket}/$location/$segmentName/")

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
      text(s"s3a://${configYaml.bucket}/$location/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  @Override
  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).text(s"s3a://${configYaml.bucket}/${location.replace(".", "/")}/")
  }

  override def writeDfPartitionDirect(df: DataFrame,location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, s"$location/$partitionName", writeMode)
  }
  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit =  throw Exception()
  @Override
  def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()

  @Override
  def getSegments(location: String): List[String] = {
    throw Exception()
  }
}
