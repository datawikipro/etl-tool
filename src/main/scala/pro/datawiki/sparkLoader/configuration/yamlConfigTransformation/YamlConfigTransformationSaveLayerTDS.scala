package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationSaveLayerTDS(
                                                 sourceName: String,
                                                 sql: String
                                               ) extends YamlConfigTransformationSaveLayerTrait {

}
