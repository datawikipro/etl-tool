package pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb

import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameTrait}
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate
import pro.datawiki.schemaValidator.json.JsonConstructor
import pro.datawiki.sparkLoader.LogMode

case class YamlConfigSchema(
                             schemaName: String,
                             fileLocation: String,
                             isError: Boolean
                           )