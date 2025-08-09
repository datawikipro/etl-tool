package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigTargetTrait}
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.task.Context
import pro.datawiki.datawarehouse.DataFrameTrait
import org.apache.spark.sql.functions.lit

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetFileSystem(
                                       connection: String,
                                       source: String,
                                       mode: String = "append",
                                       partitionMode: String,
                                       targetFile: String,
                                       partitionBy: List[String] = List.apply(),
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, partitionMode = partitionMode, source = source), YamlConfigTargetTrait {
  @JsonIgnore
  override def loader: FileStorageTrait = {
    super.loader match
      case x: FileStorageTrait => x
      case _ => throw Exception()
  }
  @JsonIgnore
  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf
    YamlConfigPartitionMode(partitionMode) match
      case YamlConfigPartitionMode.auto => {
        if partitionBy.nonEmpty then {
          loader.writeDfPartitionAuto(df.getDataFrame, targetFile, partitionBy, loadMode)
          return true
        } else {
          throw Exception()
        }
      }
      case YamlConfigPartitionMode.direct => {
        partitionBy.length match
          case 0 => loader.writeDfPartitionDirect(df.getDataFrame, targetFile, partitionBy, List.apply(), loadMode)
          case 1 => loader.writeDfPartitionDirect(df.getDataFrame, targetFile, partitionBy, List.apply(RunConfig.getPartition), loadMode)
          case _ => 
            throw Exception()
        return true
      }
      case YamlConfigPartitionMode.stream=> {
        loader.writeDf(df.getDataFrame, targetFile, loadMode)
        return true
      }
      case YamlConfigPartitionMode.streamByRunId => {
        val runId: String = "run_id"
        
        loader.writeDfPartitionAuto(
          df.getDataFrame.withColumn(runId,lit(RunConfig.getPartition)), 
          targetFile,
          List.apply(runId), 
          loadMode)
        return true
      }
      case YamlConfigPartitionMode.none => {
        loader.writeDf(df.getDataFrame, targetFile, loadMode)
        return true
      }
      case _ => {
        throw Exception()
      }
  }
}