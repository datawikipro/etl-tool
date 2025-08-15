package pro.datawiki.sparkLoader.connection.jsonApi

import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameTrait}
import pro.datawiki.schemaValidator.DataFrameConstructor
import pro.datawiki.sparkLoader.LogMode

case class YamlConfigSchema(
                             schemaName: String,
                             fileLocation: String,
                             isError: Boolean
                           ) {
  def getSchemaByJson(jsonString: String): DataFrameTrait = {
    if LogMode.isDebug then {
      println("------------------------------------------------------------------------------------------")
      println(jsonString)
      println("------------------------------------------------------------------------------------------")
    }
    if isError then {
      try {
        val df = DataFrameConstructor.getDataFrameFromJsonWithTemplate(jsonString, fileLocation)
        return DataFrameDirty(schemaName, df, false)
      } catch
        case _ => return null
    }
    val df = DataFrameConstructor.getDataFrameFromJsonWithTemplate(jsonString, fileLocation)
    return DataFrameDirty(schemaName, df, true)

  }
}