package pro.datawiki.sparkLoader.connection.minIo

import pro.datawiki.sparkLoader.connection.FileStorageType
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat.{json, parquet, text}
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.minIo.minioBatch.LoaderMinIoBatch
import pro.datawiki.yamlConfiguration.YamlClass

object LoaderMinIo extends YamlClass {
  def apply(inConfig: String, mode: FileStorageType): LoaderMinIo = {

    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])

    val loader = mode match
      case FileStorageType.parquet => new LoaderMinIoBatch(parquet,configYaml)
      case FileStorageType.json    => new LoaderMinIoBatch(json,configYaml)
      case FileStorageType.text    => new LoaderMinIoBatch(text,configYaml)
      case _ => throw Exception()

    loader.modifySpark()
    return loader
  }
}