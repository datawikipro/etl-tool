package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement

import scala.collection.mutable
import java.net.URLEncoder

class YamlConfig(
                  url: String,
                  schema: List[YamlConfigSchemaColumn] = List.apply(),
                  template: List[YamlConfigTemplate] = List.apply()
                ) {
  def getUrl: String = url

  def getSchema: List[YamlConfigSchemaColumn] = schema

  def getTemplate: List[YamlConfigTemplate] = template
  
  def process(html: WebElement): Map[String, SeleniumType] = {
    val result: mutable.Map[String, SeleniumType] = mutable.Map()
    template.foreach(i => {
      result ++= i.getSubElements(html)
    })
    return Map.apply(result.toSeq*)
  }
}


object YamlConfig {
  def apply(in: YamlConfig, row: mutable.Map[String, String]): YamlConfig = {
    if row == null then return in

    var modifiedSchema: List[YamlConfigSchemaColumn] = in.getSchema
    var modifiedUrl: String = in.getUrl

    row.foreach(j => {
      modifiedUrl = modifiedUrl.replace(s"""$${${j._1}}""", j._2)

      var newSchema: List[YamlConfigSchemaColumn] = List.apply()
      modifiedSchema.foreach(i => {
        i.getDefault match
          case null => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.getColumn, `type` = i.getType, subType = i.getSubType, default = i.getDefault)
          case _ => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.getColumn, `type` = i.getType, subType = i.getSubType, default = i.getDefault.replace(s"""$${${j._1}}""", j._2))
      })
      modifiedSchema = newSchema
    })
    return new YamlConfig(url = modifiedUrl, schema = modifiedSchema, template = in.getTemplate)
  }

  def modifyConfig(modifiedUrl: String, modifiedSchema: List[YamlConfigSchemaColumn], key: String, value: String): Unit = {

  }
}