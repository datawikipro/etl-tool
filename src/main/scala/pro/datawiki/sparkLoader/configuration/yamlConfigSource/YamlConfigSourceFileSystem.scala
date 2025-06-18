package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.task.{Context, TaskTemplate, TaskTemplateTableFromFileSystem}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

case class YamlConfigSourceFileSystem(
                                       tableName: String,
                                       tableColumns: List[YamlConfigSourceDBTableColumn],
                                       partitionBy: List[String] = List.apply(),
                                       where: String,
                                       limit: Int
                                     ) extends YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    connection match
      case x: FileStorageTrait => return TaskTemplateTableFromFileSystem(tableName = tableName, partitionBy = partitionBy, where = where, limit = limit, source = x)
      case _ => throw Exception()
  }

}