package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.task.{Context, TaskTemplate, TaskTemplateJsonApi, TaskTemplateSelenium}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

case class YamlConfigSourceWeb(
                                run: String,
                                isDirty: Boolean
                              ) extends YamlConfigSourceApiTrait, YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    connection match
      case x: LoaderSelenium => TaskTemplateSelenium(x)
      case x: LoaderJsonApi => TaskTemplateJsonApi(x)
      case _ => throw Exception()
  }

}