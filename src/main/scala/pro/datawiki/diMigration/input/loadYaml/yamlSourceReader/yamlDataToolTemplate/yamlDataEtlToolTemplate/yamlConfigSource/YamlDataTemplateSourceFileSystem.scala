package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateTableFromFileSystem}
import pro.datawiki.sparkLoader.transformation.TransformationCache

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

case class YamlDataTemplateSourceFileSystem(
                                             tableName: String,
                                             tableColumns: List[YamlConfigSourceDBTableColumn],
                                             partitionBy: List[String] = List.apply(),
                                             where: String,
                                             limit: Int
                                           )