package pro.datawiki.sparkLoader

import pro.datawiki.yamlConfiguration.YamlClass

case class ExternalProgressLoggingConfig(
                                          connection: String,
                                          configLocation: String
                                        )

object ExternalProgressLoggingConfig extends YamlClass {
  def apply(yamlPath: String): ExternalProgressLoggingConfig = {
    try {
      val result: ExternalProgressLoggingConfig = mapper.readValue(getLines(yamlPath), classOf[ExternalProgressLoggingConfig])
      return result
    } catch {
      case e: com.fasterxml.jackson.core.JsonParseException =>
        throw new IllegalArgumentException(s"Ошибка синтаксиса при загрузке external progress logging из: $yamlPath - ${e.getMessage}", e)
      case e: com.fasterxml.jackson.databind.JsonMappingException =>
        throw new IllegalArgumentException(s"Ошибка структуры при загрузке external progress logging из: $yamlPath - ${e.getMessage}", e)
      case e: java.io.IOException =>
        throw new IllegalArgumentException(s"Ошибка ввода/вывода при загрузке external progress logging из: $yamlPath - ${e.getMessage}", e)
      case e: Exception =>
        throw new IllegalArgumentException(s"Непредвиденная ошибка при загрузке external progress logging из: $yamlPath - ${e.getMessage}", e)
    }
  }
}
