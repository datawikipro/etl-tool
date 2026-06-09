package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement

import scala.collection.mutable

class YamlConfig(
                  url: String,
                  schema: List[YamlConfigSchemaColumn] = List.apply(),
                  template: List[YamlConfigTemplate] = List.apply()
                ) {
  def getSeleniumConfig: YamlConfig = this

  def getUrl: String = url

  def getSchema: List[YamlConfigSchemaColumn] = schema

  def getTemplate: List[YamlConfigTemplate] = template

  def getModifiedUrl(parameters: Map[String, String]): String = {
    return YamlConfig.getModifiedString(url, parameters)
  }

  def getModifiedSchema(parameters: Map[String, String]): List[YamlConfigSchemaColumn] = {
    var newSchema: List[YamlConfigSchemaColumn] = List.apply()
    schema.foreach(i => {
      newSchema = newSchema.appended(i.getModified(parameters))
    })
    return newSchema
  }

  def getModifiedTemplate(parameters: Map[String, String]): List[YamlConfigTemplate] = {
    var newTemplate: List[YamlConfigTemplate] = List.apply()
    template.foreach(i => {
      newTemplate = newTemplate.appended(i.getModified(parameters))
    })
    return newTemplate
  }

  def process(html: WebElement): Map[String, SeleniumType] = {
    val result: mutable.Map[String, SeleniumType] = mutable.Map()
    template.zipWithIndex.foreach((i, index) => {
      val a = i.getSubElements(html)
      result ++= a
    })
    return Map.apply(result.toSeq *)
  }
}


object YamlConfig {
  def apply(in: YamlConfig, row: Map[String, String]): YamlConfig = {
    if row == null then return in
    return new YamlConfig(url = in.getModifiedUrl(row), schema = in.getModifiedSchema(row), template = in.getModifiedTemplate(row))
  }

  def getModifiedString(string: String, parameters: Map[String, String]): String = {
    if string == null then return null
    var modifiedString: String = string
    parameters.foreach(j => {
      modifiedString = modifiedString.replace(s"""$${${j._1}}""", j._2)
    })
    return modifiedString
  }
}