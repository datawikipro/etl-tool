package pro.datawiki.schemaValidator.projectSchema

import pro.datawiki.exception.SchemaValidationException
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaStruct

trait SchemaTrait {
  def test(): Any = throw SchemaValidationException("test method not implemented in SchemaTrait")
}