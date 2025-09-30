package pro.datawiki.sparkLoader.connection.mail

import pro.datawiki.sparkLoader.connection.selenium.{YamlConfig, YamlConfigSchemaColumn, YamlConfigTemplate}

case class YamlConfigMail(
                           host: String,
                           port: String,
                           protocol: String,
                           schema: List[YamlConfigSchemaColumn] = List.apply(),
                           template: List[YamlConfigTemplate] = List.apply()
                         ) extends YamlConfig(url = null, schema = schema, template = template) {

}
