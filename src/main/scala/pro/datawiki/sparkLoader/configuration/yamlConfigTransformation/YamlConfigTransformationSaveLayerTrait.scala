package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import pro.datawiki.exception.NotImplementedException

trait YamlConfigTransformationSaveLayerTrait {
  def save(): Unit = {
    throw NotImplementedException("save method not implemented for YamlConfigTransformationSaveLayerTrait")
  }
}
