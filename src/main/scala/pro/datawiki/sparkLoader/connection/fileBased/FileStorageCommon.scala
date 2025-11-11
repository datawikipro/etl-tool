package pro.datawiki.sparkLoader.connection.fileBased

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.sql.functions.lit
import pro.datawiki.sparkLoader.SparkObject

object FileStorageCommon {

  def buildPartitionPostfix(keyPartitions: List[String], valuePartitions: List[String]): String = {
    var postfix: String = ""
    keyPartitions.zipWithIndex.foreach { case (_, index) =>
      postfix += s"${keyPartitions(index)}=${valuePartitions(index)}/"
    }
    postfix
  }

  def s3aUri(bucket: String, location: String): String = s"s3a://$bucket/$location"

  def buildPartitionedLocation(baseLocation: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
    val postfix = buildPartitionPostfix(keyPartitions, valuePartitions)
    s"$baseLocation/${postfix}"
  }

  def createDataFrameReader(
    format: FileBaseFormat,
    corruptRecordColumn: Option[String] = None,
    jsonMode: Option[String] = None,
    jsonMultiline: Option[Boolean] = None
  ): DataFrameReader = {
    var reader = SparkObject.spark.read.format(format.toString)
    format match {
      case FileBaseFormat.`json` =>
        if (corruptRecordColumn.nonEmpty) reader = reader.option("columnNameOfCorruptRecord", corruptRecordColumn.get)
        if (jsonMode.nonEmpty) reader = reader.option("mode", jsonMode.get)
        if (jsonMultiline.nonEmpty) reader = reader.option("multiline", jsonMultiline.get.toString)
        reader
      case FileBaseFormat.`parquet` =>
        reader.option("mergeSchema", "true")
      case FileBaseFormat.`csv` =>
        reader.option("header", "true").option("inferSchema", "true")
      case _ => reader
    }
  }

  def appendPartitionColumns(    df: DataFrame,    keyPartitions: List[String],    valuePartitions: List[String]  ): DataFrame = {
    var result = df
    keyPartitions.zipWithIndex.foreach { case (_, index) =>
      result = result.withColumn(keyPartitions(index), lit(valuePartitions(index)))
    }
    result
  }

  def optimizeDataFramePartitions(df: DataFrame): DataFrame = {
    try {
      val rowCount = df.count()
      val currentPartitions = df.rdd.getNumPartitions

      // Aim for ~200000 rows per partition, cap total partitions to 50
      val desiredByRows = Math.max(1, Math.ceil(rowCount / 200000.0).toInt)//TODO
      val targetPartitions = desiredByRows

      if (targetPartitions != currentPartitions) {
        if (targetPartitions < currentPartitions) {
          return df.coalesce(targetPartitions)
        } else {
          return df.repartition(targetPartitions)
        }
      } else {
        return df
      }
    } catch {
      case _: Exception =>
        return df
    }
  }
}


