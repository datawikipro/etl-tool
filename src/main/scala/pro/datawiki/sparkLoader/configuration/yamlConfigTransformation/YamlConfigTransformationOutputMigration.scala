package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

case class YamlConfigTransformationOutputMigration(
                                                    sourceName: String,
                                                    sql: String
                                                  ) extends YamlConfigTransformationSaveLayerTrait {

}
