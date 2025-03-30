package pro.datawiki.sparkLoader.connection.minIo.minioJson

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.{DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheFileStorage}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import org.apache.spark.sql.functions.lit

class LoaderMinIoJson(configYaml: YamlConfig) extends LoaderMinIo(configYaml), DataWarehouseTrait, FileStorageTrait {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in,inLocation)
  private val cache: TransformationCacheFileStorage = new TransformationCacheFileStorage(this)

  override def getFolder(location: String): List[String] = super.getFolder(configYaml.bucket,location)

  @Override
  def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(s"s3a://${configYaml.bucket}/$location/$segmentName/")

    LogMode.debugDF(df)
    return df
  }

  @Override
  def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(super.getLocation(location = location))

    LogMode.debugDF(df)
    return df
  }

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    var df: DataFrame = SparkObject.spark.read.json(super.getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions))
    LogMode.debugDF(df)
    keyPartitions.zipWithIndex.foreach { case (value, index) =>
      df = df.withColumn(keyPartitions(index), lit(valuePartitions(index)))
    }
    LogMode.debugDF(df)
    return df
  }

  @Override
  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    val target = s"${configYaml.bucket}/${location.replace(".", "/")}/"
    df.write.mode(writeMode.toString).json(s"s3a://$target")

  }

  @Override
  def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, s"$location/${partitionName.mkString("/")}", writeMode)
  }

  @Override
  def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    if writeMode == overwrite then {
      df.orderBy(partitionName.head, partitionName *).write.partitionBy(partitionName *).mode(writeMode.toString).json(s"s3a://${configYaml.bucket}/${location.replace(".", "/")}/")
      return
    }
    cache.saveTablePartitionAuto(df = df, partitionName = partitionName)
    cache.moveTablePartition(configYaml.bucket, s"${location.replace(".", "/")}/", partitionName)
  }

  @Override
  override def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = {
    super.moveTablePartition(oldTableSchema, oldTable, newTable, partitionName, writeMode)
  }


  @Override
  def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()

  @Override
  def getSegments(location: String): List[String] = {
    throw Exception()
  }

  override def getMasterFolder: String = super.getBucketName
}
