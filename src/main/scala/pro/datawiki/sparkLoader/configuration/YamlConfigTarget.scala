package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.WriteMode

case class YamlConfigTarget(connection: String,
                            targetFile: String,
                            autoInsertIdmapCCD: Boolean,
                            columns: List[YamlConfigTargetColumn],
                            uniqueKey: List[String] = List.apply(),
                            partitionKey: String,
                            mode: String = "append"
                           ) {
  def getMode: WriteMode = {
    mode match
      case "overwrite" => WriteMode.overwrite
      case "append" => WriteMode.append
      case _ => throw Exception()
  }

  def getColumns: List[String] = {
    var list: List[String] = List.apply()
    columns.foreach(i => list = list.appended(i.columnName))
    return list diff uniqueKey
  }
}