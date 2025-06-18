package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigTargetTrait}
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.task.Context
import pro.datawiki.datawarehouse.{DataFrameTrait}

case class YamlConfigTargetFileSystem(
                                       connection: String,
                                       source: String,
                                       mode: String = "append",
                                       partitionMode: String,
                                       targetFile: String,
                                       partitionBy: List[String] = List.apply(),
                                     ) extends YamlConfigTargetBase(connection = connection, mode = mode, partitionMode = partitionMode, source = source), YamlConfigTargetTrait {

  override def loader: FileStorageTrait = {
    super.loader match
      case x: FileStorageTrait => x
      case _ => throw Exception()
  }

  override def writeTarget(): Boolean = {
    val df: DataFrameTrait = getSourceDf
    partitionMode match
      case "auto" => {
        if partitionBy.nonEmpty then {
          loader.writeDfPartitionAuto(df.getDataFrame, targetFile, partitionBy, loadMode)
          return true
        } else {
          throw Exception()
        }
      }
      case "direct" => {
        partitionBy.length match
          case 0 => loader.writeDfPartitionDirect(df.getDataFrame, targetFile, partitionBy, List.apply(), loadMode)
          case 1 => loader.writeDfPartitionDirect(df.getDataFrame, targetFile, partitionBy, List.apply(RunConfig.getPartition), loadMode)
          case _ => 
            throw Exception()
        return true
      }
      case null => {
        loader.writeDf(df.getDataFrame, targetFile, loadMode)
        return true
      }
      case _ => throw Exception()
  }
}