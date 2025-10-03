package pro.datawiki.sparkLoader.connection.minIo

import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.connection.minIo.minioStream.LoaderMinIoJsonStream
import pro.datawiki.sparkLoader.dictionaryEnum.FileStorageType
import pro.datawiki.yamlConfiguration.YamlClass

object LoaderMinIoStream extends YamlClass {
  def apply(inConfig: String, mode: FileStorageType): LoaderMinIo = {

    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])

    val loader = mode match
      //      case FileStorageType.parquet => new LoaderMinIoStreamParquet(configYaml)
      case FileStorageType.json => new LoaderMinIoJsonStream(FileBaseFormat.json, configYaml, inConfig)
      //      case FileStorageType.text => new LoaderMinIoStreamText(configYaml)
      case _ => throw UnsupportedOperationException("Unsupported file storage type for MinIO Stream")

    loader.modifySpark()
    return loader
  }
}