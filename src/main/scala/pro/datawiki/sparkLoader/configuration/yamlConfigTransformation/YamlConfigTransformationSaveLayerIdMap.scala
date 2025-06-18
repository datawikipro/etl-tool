package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationSaveLayerIdMap(
                                                   sourceName: String,
                                                   sql: String
                                                 ) extends YamlConfigTransformationSaveLayerTrait {

}                