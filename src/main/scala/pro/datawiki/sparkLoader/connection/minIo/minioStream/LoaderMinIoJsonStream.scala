package pro.datawiki.sparkLoader.connection.minIo.minioStream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

class LoaderMinIoJsonStream(format: FileBaseFormat, configYaml: YamlConfig) extends LoaderMinIo(format, configYaml) with FileStorageTrait {

  @Override
  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    val target = s"${configYaml.bucket}/$location/"
    val query = df.writeStream
      .format(format.toString)
      .option("path", s"s3a://$target")
      .option("checkpointLocation", s"/opt/etl-tool/kafka/$location/")
      .option("endingOffsets", "latest")
      .trigger(Trigger.Once())
      .start()
    query.awaitTermination()
  }

  @Override
  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    val zipped = partitionName.zip(partitionValue)
    var partition = ""
    zipped.foreach { case (num, char) =>
      partition += s"$num=$char/"
    }
    val target = s"${configYaml.bucket}/$location/${partition}"
    val query = df.writeStream
      .format(format.toString)
      .option("path", s"s3a://$target")
      .option("checkpointLocation", s"/opt/etl-tool/kafka/$location/")
      .option("endingOffsets", "latest")
      .trigger(Trigger.Once())
      .start()
    query.awaitTermination()
  }

  @Override
  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    val target = s"${configYaml.bucket}/$location/"
    val query = df.writeStream
      .format(format.toString)
      .option("path", s"s3a://$target")
      .option("checkpointLocation", s"/opt/etl-tool/kafka/$location/")
      .option("endingOffsets", "latest")
      .trigger(Trigger.Once())
      .partitionBy(partitionName *)
      .start()
    query.awaitTermination()
  }

  override def close(): Unit = {
  }

}
