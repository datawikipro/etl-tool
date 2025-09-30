package pro.datawiki.exception

class SchemaValidationException(message: String = "Schema validation error") extends Exception(message)

object SchemaValidationException {
  def apply(message: String): SchemaValidationException = {
    new SchemaValidationException(message)
  }

  def apply(message: String, e: Exception): SchemaValidationException = {
    SchemaValidationException(s"$message\n${e.getMessage}")
  }
}