package pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation

import pro.datawiki.sparkLoader.connection.FileStorageTrait

case class YamlConfigEltOnServerColdDataFileBasedFolder(
                                                         targetFile: String,
                                                         tableName:String,
                                                         partitionBy: List[String] = List.apply(),
                                                       ) {

  def getListPartition(x: FileStorageTrait): List[String] = {
    var list = x.getFolder(targetFile)
    partitionBy.length match {
      case 1 => {
        list=  list.
          filter(col => col.contains(s"$targetFile/${partitionBy.head}=")).
          map(col => col.replace(s"$targetFile/${partitionBy.head}=","").split("/").head).
          distinct

        return list
      }
      case _ => {
        throw Exception()
      }
    }

  }
}
