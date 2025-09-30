package pro.datawiki.yamlConfiguration

import ch.qos.logback.classic.{Logger, LoggerContext}
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.textVariable.WorkWithText

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable


class YamlClass {

  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

  mapper.registerModule(DefaultScalaModule)
  mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
  mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_SELF_REFERENCES, false)
  mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_NULL_MAP_VALUES, false)

  // Configure to handle circular references by using reference handling
  mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL, true)

  // Add mixins to handle Logger classes that have circular references
  mapper.addMixIn(classOf[Logger], classOf[LoggerMixin])
  mapper.addMixIn(classOf[LoggerContext], classOf[LoggerContextMixin])
  mapper.addMixIn(classOf[org.slf4j.Logger], classOf[SLF4JLoggerMixin])
  mapper.addMixIn(classOf[pro.datawiki.sparkLoader.traits.LoggingTrait], classOf[LoggingTraitMixin])


  def getLines(inConfig: String): String = YamlClass.getLines(inConfig)
  def getLinesGlobalContext(inConfig: String): String = YamlClass.getLines(inConfig,ApplicationContext.getGlobalVariables)

  def getLines(inConfig: String, row: mutable.Map[String, String]): String = YamlClass.getLines(inConfig, row)


  def toYaml(obj: Any): String = {
    try {
      mapper.writeValueAsString(obj)
    } catch {
      case e: com.fasterxml.jackson.core.JsonProcessingException =>
        throw ConfigurationException(s"Ошибка при сериализации объекта в YAML: ${e.getMessage}", e)
      case e: Exception =>
        throw ConfigurationException(s"Непредвиденная ошибка при сериализации объекта в YAML: ${e.getMessage}", e)
    }
  }
}

object YamlClass {
  def toYaml(obj: Any): String = {
    new YamlClass().toYaml(obj)
  }

  def writefile(fileLocation: String, text: String): Unit = {
    try {
      val filePath = Paths.get(fileLocation)
      Files.createDirectories(filePath.getParent)
      Files.write(filePath, text.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: java.nio.file.InvalidPathException =>
        throw ConfigurationException(s"Недопустимый путь к файлу: $fileLocation", e)
      case e: java.nio.file.FileSystemException =>
        throw ConfigurationException(s"Ошибка файловой системы при записи файла: $fileLocation - ${e.getMessage}", e)
      case e: java.io.IOException =>
        throw ConfigurationException(s"Ошибка ввода/вывода при записи файла: $fileLocation - ${e.getMessage}", e)
      case e: Exception =>
        throw ConfigurationException(s"Непредвиденная ошибка при записи файла: $fileLocation - ${e.getMessage}", e)
    }
  }

  def deleteFile(fileLocation: String): Unit = {
    val filePath = Paths.get(fileLocation)
    Files.delete(filePath)
  }

  def getLines(inConfig: String): String = {
    try {
      val a = Files.readString(Paths.get(inConfig))
      return a
    } catch {
      case e: java.nio.file.NoSuchFileException =>
        throw ConfigurationException(s"Файл конфигурации не найден: $inConfig", e)
      case e: java.io.IOException =>
        throw ConfigurationException(s"Ошибка при чтении файла конфигурации: $inConfig - ${e.getMessage}", e)
      case e: Exception =>
        throw ConfigurationException(s"Непредвиденная ошибка при чтении файла конфигурации: $inConfig", e)
    }
  }

  def getLines(inConfig: String, row: mutable.Map[String, String]): String = {
    try {
      val a = Files.readString(Paths.get(inConfig))
      return WorkWithText.replaceWithoutDecode(a, row)
    } catch {
      case e: java.nio.file.NoSuchFileException =>
        throw ConfigurationException(s"Файл конфигурации не найден: $inConfig", e)
      case e: java.io.IOException =>
        throw ConfigurationException(s"Ошибка при чтении файла конфигурации: $inConfig - ${e.getMessage}", e)
      case e: Exception =>
        throw ConfigurationException(s"Непредвиденная ошибка при чтении или обработке файла конфигурации: $inConfig", e)
    }
  }
}

// Mixin classes to handle Logger serialization - ignore all logger fields
@JsonIgnoreProperties(Array("name", "level", "effectiveLevel", "errorEnabled", "warnEnabled", "infoEnabled", "debugEnabled", "traceEnabled", "additive", "loggerContext", "loggerList", "parent", "children", "appenderList", "statusManager", "configurationLock", "scheduledExecutorService", "logger"))
trait LoggerMixin

@JsonIgnoreProperties(Array("loggerList", "parent", "children", "appenderList", "statusManager", "configurationLock", "scheduledExecutorService", "logger"))
trait LoggerContextMixin

@JsonIgnoreProperties(Array("name", "level", "effectiveLevel", "errorEnabled", "warnEnabled", "infoEnabled", "debugEnabled", "traceEnabled", "additive", "loggerContext", "loggerList", "parent", "children", "appenderList", "statusManager", "configurationLock", "scheduledExecutorService", "logger"))
trait SLF4JLoggerMixin

@JsonIgnoreProperties(Array("logger", "etlProgressLogger"))
trait LoggingTraitMixin