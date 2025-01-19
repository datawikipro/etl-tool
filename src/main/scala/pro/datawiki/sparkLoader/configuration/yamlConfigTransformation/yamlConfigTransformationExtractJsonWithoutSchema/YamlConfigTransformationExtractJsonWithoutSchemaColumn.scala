package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationExtractJsonWithoutSchema

class YamlConfigTransformationExtractJsonWithoutSchemaColumn(columnName: String,
                                                                  newColumnName: String,
                                                                  columnType: String,
                                                                  subColumns: List[YamlConfigTransformationExtractJsonWithoutSchemaColumn],
                                                                  action: String
                                                                 ) {
  def getColumnName: String = columnName
  def getNewColumnName: String={
    newColumnName match
      case null => columnName
      case _ => newColumnName
  }
  def getColumnType: String= columnType
  def getSubColumns: List[YamlConfigTransformationExtractJsonWithoutSchemaColumn] = {
    if subColumns == null then {
      throw Exception()
    }
    return subColumns
  }
  def getAction: String= action
}
