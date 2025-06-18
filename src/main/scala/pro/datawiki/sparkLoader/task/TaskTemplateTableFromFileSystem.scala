package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateTableFromFileSystem(tableName: String,
                                      partitionBy: List[String] = List.apply(),
                                      where: String,
                                      limit: Int, source: FileStorageTrait) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    var df = partitionBy.length match
      case 0 => source.readDf(tableName)
      case 1 => source.readDf(tableName, partitionBy, List.apply(RunConfig.getPartition))
      case 2 => source.readDf(tableName, partitionBy, List.apply(RunConfig.getPartition, RunConfig.getSubPartition))
      case _ => throw Exception()
    
    if where != null then df = df.where(where)
    if limit > 0 then df = df.limit(limit)
    return List.apply(DataFrameOriginal(df))
  }

}
