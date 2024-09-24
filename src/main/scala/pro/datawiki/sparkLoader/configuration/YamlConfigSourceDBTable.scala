package pro.datawiki.sparkLoader.configuration

case class YamlConfigSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn]
                                  ){
  def getColumnNames: List[String] = {
    var lst :List[String] = List.empty
    tableColumns.foreach(i=>
      //lst = lst.appended(f"cast(${i.columnName} as String) as ${i.columnName}")
      lst = lst.appended(i.columnName)
    )
    return lst
  }

}