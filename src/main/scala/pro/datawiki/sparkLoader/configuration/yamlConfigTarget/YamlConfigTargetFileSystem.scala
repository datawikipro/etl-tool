package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.functions.lit
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.traits.LoggingTrait

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetFileSystem(
                                       connection: String,
                                       source: String,
                                       tableName:String,
                                       mode: String = "append",
                                       targetFile: String,
                                       partitionBy: List[String] = List.apply(),
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, source = source) with YamlConfigTargetTrait with LoggingTrait {

  @JsonIgnore
  override def loader: FileStorageTrait = {
    super.loader match
      case x: FileStorageTrait => x
      case _ => throw IllegalArgumentException("Invalid loader type")
  }

  def writeAutoPartition(df: DataFrameTrait, mode: WriteMode): Boolean = {
    if partitionBy.isEmpty then throw IllegalArgumentException("partitionBy cannot be empty for auto partition mode")

    loader.writeDfPartitionAuto(df.getDataFrame, tableName, targetFile, partitionBy, mode)
    return true

  }

  def writeDirectPartition(df: DataFrameTrait): Boolean = {
    loader.writeDfPartitionDirect(df.getDataFrame, tableName, targetFile, partitionBy, ApplicationContext.getPartitions(partitionBy *), loadMode, false)
    return true
  }

  def writeStream(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, targetFile, loadMode)
    return true
  }


  def writeStreamByRunId(df: DataFrameTrait): Boolean = {
    val listPartition: List[String] = List.apply("run_id")

    var dfWithExtraColumn = df.getDataFrame
    ApplicationContext.getPartitions2(listPartition *).foreach(col => {
      dfWithExtraColumn = dfWithExtraColumn.withColumn(col._1, lit(col._2))
    })

    loader.writeDfPartitionAuto(dfWithExtraColumn, tableName, targetFile, listPartition, loadMode)

    return true
  }

  def writeFullTable(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, tableName, targetFile, loadMode)
    return true
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf

    WriteMode(mode) match {
      case WriteMode.autoOverwrite => writeAutoPartition(df, WriteMode.overwriteTable)
      case WriteMode.autoAppend => writeAutoPartition(df, WriteMode.append)
      //      case WriteMode.direct => writeDirectPartition(df)
      case WriteMode.stream => writeStream(df)
      case WriteMode.streamByRunId => writeStreamByRunId(df)
      case WriteMode.overwriteTable => writeDirectPartition(df)
      case WriteMode.overwritePartition => writeDirectPartition(df)
      //      case WriteMode.none => writeFullTable(df)

      case fs => {
        throw UnsupportedOperationException(s"Unsupported partition mode: $fs")
      }
    }
  }
}