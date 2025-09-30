package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationSaveLayerSTG(
                                                 sourceName: String,
                                                 sql: String
                                               ) extends YamlConfigTransformationSaveLayerTrait {

}