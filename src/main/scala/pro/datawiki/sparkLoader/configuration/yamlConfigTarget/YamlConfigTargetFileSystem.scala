package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.functions.lit
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetFileSystem.YamlConfigPartitionMode
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.traits.LoggingTrait

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetFileSystem(
                                       connection: String,
                                       source: String,
                                       mode: String = "append",
                                       partitionMode: String,
                                       targetFile: String,
                                       partitionBy: List[String] = List.apply(),
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, source = source) with YamlConfigTargetTrait with LoggingTrait {

  @JsonIgnore
  override def loader: FileStorageTrait = {
    super.loader match
      case x: FileStorageTrait => x
      case _ => throw IllegalArgumentException("Invalid loader type")
  }

  def writeAutoPartition(df: DataFrameTrait): Boolean = {
    if partitionBy.nonEmpty then {
      loader.writeDfPartitionAuto(df.getDataFrame, targetFile, partitionBy, loadMode)
      return true
    } else {
      throw IllegalArgumentException("partitionBy cannot be empty for auto partition mode")
    }
  }

  def writeDirectPartition(df: DataFrameTrait): Boolean = {
    loader.writeDfPartitionDirect(df.getDataFrame, targetFile, partitionBy, ApplicationContext.getPartitions(partitionBy *), loadMode)
    return true
  }

  def writeStream(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, targetFile, loadMode)
    return true
  }


  def writeStreamByRunId(df: DataFrameTrait): Boolean = {
    val listPartition: List[String] = List.apply("run_id")

    var dfWithExtraColumn = df.getDataFrame
    ApplicationContext.getPartitions2(listPartition *).foreach(col => {
      dfWithExtraColumn = dfWithExtraColumn.withColumn(col._1, lit(col._2))
    })

    loader.writeDfPartitionAuto(dfWithExtraColumn, targetFile, listPartition, loadMode)

    return true
  }

  def writeFullTable(df: DataFrameTrait): Boolean = {
    loader.writeDf(df.getDataFrame, targetFile, loadMode)
    return true
  }

  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf

    YamlConfigPartitionMode(partitionMode) match {
      case YamlConfigPartitionMode.auto => writeAutoPartition(df)
      case YamlConfigPartitionMode.direct => writeDirectPartition(df)
      case YamlConfigPartitionMode.stream => writeStream(df)
      case YamlConfigPartitionMode.streamByRunId => writeStreamByRunId(df)
      case YamlConfigPartitionMode.none => writeFullTable(df)
      case _ => {
        throw UnsupportedOperationException("Unsupported partition mode")
      }
    }
  }
}