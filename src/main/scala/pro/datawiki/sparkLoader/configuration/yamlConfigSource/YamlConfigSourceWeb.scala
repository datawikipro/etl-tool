package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb.YamlConfigSchema
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateJsonApi, TaskTemplateSelenium}

case class YamlConfigSourceWeb(
                                run: String,
                                isDirty: Boolean,
                                schemas: List[YamlConfigSchema] = List.apply(),
                              ) extends YamlConfigSourceApiTrait, YamlConfigSourceTrait {

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    connection match
      case x: LoaderSelenium => TaskTemplateSelenium(x)
      case x: LoaderJsonApi => TaskTemplateJsonApi(x,schemas)
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for Web source: ${connection.getClass.getSimpleName}")
  }

}