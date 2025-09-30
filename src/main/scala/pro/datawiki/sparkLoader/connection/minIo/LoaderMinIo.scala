package pro.datawiki.sparkLoader.connection.minIo

import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.minIo.minioBatch.LoaderMinIoBatch
import pro.datawiki.sparkLoader.dictionaryEnum.FileStorageType
import pro.datawiki.yamlConfiguration.YamlClass

object LoaderMinIo extends YamlClass {
  def apply(inConfig: String, mode: FileStorageType): LoaderMinIo = {

    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])

    val loader = mode match
      case FileStorageType.parquet => new LoaderMinIoBatch(FileBaseFormat.parquet, configYaml)
      case FileStorageType.json => new LoaderMinIoBatch(FileBaseFormat.json, configYaml)
      case FileStorageType.text => new LoaderMinIoBatch(FileBaseFormat.text, configYaml)
      case _ => throw UnsupportedOperationException("Unsupported file storage type for MinIO")

    loader.modifySpark()
    return loader
  }
}