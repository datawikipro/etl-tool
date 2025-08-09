package pro.datawiki.schemaValidator

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate
import pro.datawiki.schemaValidator.jsonSchema.JsonSchema
import pro.datawiki.yamlConfiguration.YamlClass


@main
def sparkRun(in: String): Unit = {
  println(in)
  val jsonSchema = JsonSchema(List.apply(in))
  var schema: BaseSchemaTemplate = jsonSchema.getBaseSchemaTemplate

  if (schema == null) {
    throw SchemaValidationException("Не удалось создать схему - возможно, входные JSON данные отсутствуют или все содержат ошибки")
  }

  val project = YamlClass.toYaml(schema.getProjectSchema)
  println(project)
}