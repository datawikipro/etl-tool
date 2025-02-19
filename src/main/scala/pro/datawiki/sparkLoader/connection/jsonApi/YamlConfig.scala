package pro.datawiki.sparkLoader.connection.jsonApi

import scala.reflect.io.File

case class YamlConfig(
                  url: String,
                  schema: YamlConfigSchema,
                  apiToken: String,   
                  cookies: List[KeyValue] = List.apply()
                ) {
  def getUrl: String = url
  //def getSchema:String = schema.getSchema
  def getSchemaFile:String = schema.getSchemaFile
}