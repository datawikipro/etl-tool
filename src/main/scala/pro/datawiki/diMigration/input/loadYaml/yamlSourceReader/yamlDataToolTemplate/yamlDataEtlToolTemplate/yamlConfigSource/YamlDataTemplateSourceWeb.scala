package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.task.{Context, TaskTemplate, TaskTemplateJsonApi, TaskTemplateSelenium}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

case class YamlDataTemplateSourceWeb(
                                run: String,
                                isDirty: Boolean
                              )