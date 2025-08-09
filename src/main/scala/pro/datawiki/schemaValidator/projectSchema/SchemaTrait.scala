package pro.datawiki.schemaValidator.projectSchema

import pro.datawiki.schemaValidator.baseSchema.BaseSchemaStruct
import pro.datawiki.exception.SchemaValidationException

trait SchemaTrait {
  def test(): Any = throw SchemaValidationException("test method not implemented in SchemaTrait")
}