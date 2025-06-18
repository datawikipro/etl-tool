package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

class YamlConfigTransformationIdMapConfig(
                                                systemCode: String,
                                                columnNames: List[String],
                                                domainName: String,
                                                alias: String
                                              ) extends YamlConfigTransformationIdMapBaseConfig(systemCode, columnNames, domainName){
  def getAlias:String = {
    if alias == null then return domainName
    return alias
  }
}