package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationSaveLayerODS(
                                                 sourceName: String,
                                                 sql: String
                                               ) extends YamlConfigTransformationSaveLayerTrait {

}