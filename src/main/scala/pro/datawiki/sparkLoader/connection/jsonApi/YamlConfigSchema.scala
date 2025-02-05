package pro.datawiki.sparkLoader.connection.jsonApi

import pro.datawiki.sparkLoader.YamlClass

case class YamlConfigSchema(
                  fileLocation: String
                ) {
  //def getSchema:String = getLines(fileLocation)
  def getSchemaFile: String = fileLocation
}