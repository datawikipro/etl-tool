package pro.datawiki.sparkLoader.connection.minIo.minioBatch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.{DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCacheFileStorage
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

class LoaderMinIoBatch(format:FileBaseFormat,configYaml: YamlConfig) extends LoaderMinIo(format,configYaml), DataWarehouseTrait, FileStorageTrait {
  private val cache: TransformationCacheFileStorage = new TransformationCacheFileStorage(this)

  @Override
  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.format(format.toString).mode(writeMode.toString).save(getLocation(location))
  }

  @Override
  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    df.write.format(format.toString).mode(writeMode.toString).save(this.getLocation(location, partitionName, partitionValue))
  }

  @Override
  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    if writeMode == overwrite then {
      df.orderBy(partitionName.head, partitionName *).write.format(format.toString).option("nullValue", "\\N").partitionBy(partitionName *).mode(writeMode.toString).save(s"s3a://${configYaml.bucket}/${location}/")
      return
    }
    cache.saveTablePartitionAuto(df = df, partitionName = partitionName)
    cache.moveTablePartition(configYaml.bucket, s"${location}/", partitionName)
  }

  override def close(): Unit = {
    cache.close()
  }
}
