package pro.datawiki.sparkLoader.connection.minIo

import pro.datawiki.sparkLoader.connection.FileStorageType
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson
import pro.datawiki.sparkLoader.connection.minIo.minioParquet.LoaderMinIoParquet
import pro.datawiki.sparkLoader.connection.minIo.minionText.LoaderMinIoText
import pro.datawiki.yamlConfiguration.YamlClass

object LoaderMinIo extends YamlClass {
  def apply(inConfig: String, mode: FileStorageType): LoaderMinIo = {

    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])

    val loader = mode match
      case FileStorageType.parquet => new LoaderMinIoParquet(configYaml)
      case FileStorageType.json => new LoaderMinIoJson(configYaml)
      case FileStorageType.text => new LoaderMinIoText(configYaml)
      case _ => throw Exception()

    loader.modifySpark()
    return loader
  }
}