package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationSaveLayerDDS(
                                                 sourceName: String,
                                                 sql: String
                                               ) extends YamlConfigTransformationSaveLayerTrait {

}
