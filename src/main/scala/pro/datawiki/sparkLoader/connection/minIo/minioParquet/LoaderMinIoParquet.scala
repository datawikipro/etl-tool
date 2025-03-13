package pro.datawiki.sparkLoader.connection.minIo.minioParquet

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}

class LoaderMinIoParquet(configYaml: YamlConfig) extends LoaderMinIo(configYaml), DataWarehouseTrait, FileStorageTrait {

  override def getFolder(location: String): List[String] = super.getFolder(configYaml.bucket, location)

  @Override
  def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.parquet(s"s3a://${configYaml.bucket}/$location/$segmentName/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  @Override
  def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.parquet(s"s3a://${configYaml.bucket}/$location/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.parquet(super.getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions))

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  @Override
  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).parquet(s"s3a://${configYaml.bucket}/${location.replace(".", "/")}/")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, s"$location/$partitionName", writeMode)
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw Exception()

  @Override
  def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()

  @Override
  def getSegments(location: String): List[String] = {
    throw Exception()
  }
}
