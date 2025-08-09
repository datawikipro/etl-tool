package pro.datawiki.yamlConfiguration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.textVariable.WorkWithText

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable


class YamlClass {

  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

  mapper.registerModule(DefaultScalaModule)


  def getLines(inConfig: String): String = YamlClass.getLines(inConfig)


  def getLines(inConfig: String, row: mutable.Map[String, String]): String = YamlClass.getLines(inConfig,row)


  def toYaml(obj: Any): String = {
    try {
      mapper.writeValueAsString(obj)
    } catch {
      case e: com.fasterxml.jackson.core.JsonProcessingException =>
        throw new ConfigurationException(s"Ошибка при сериализации объекта в YAML: ${e.getMessage}", e)
      case e: Exception =>
        throw new ConfigurationException(s"Непредвиденная ошибка при сериализации объекта в YAML: ${e.getMessage}", e)
    }
  }
}

object YamlClass {
  def toYaml(obj: Any): String ={
    new YamlClass().toYaml(obj)
  }

  def writefile(fileLocation: String, text: String): Unit = {
    try {
      val filePath = Paths.get(fileLocation)
      Files.createDirectories(filePath.getParent)
      Files.write(filePath, text.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: java.nio.file.InvalidPathException =>
        throw new ConfigurationException(s"Недопустимый путь к файлу: $fileLocation", e)
      case e: java.nio.file.FileSystemException =>
        throw new ConfigurationException(s"Ошибка файловой системы при записи файла: $fileLocation - ${e.getMessage}", e)
      case e: java.io.IOException =>
        throw new ConfigurationException(s"Ошибка ввода/вывода при записи файла: $fileLocation - ${e.getMessage}", e)
      case e: Exception =>
        throw new ConfigurationException(s"Непредвиденная ошибка при записи файла: $fileLocation - ${e.getMessage}", e)
    }
  }
  
  def getLines(inConfig: String): String = {
    try {
      val a = Files.readString(Paths.get(inConfig))
      return a
    } catch {
      case e: java.nio.file.NoSuchFileException => 
        throw new ConfigurationException(s"Файл конфигурации не найден: $inConfig", e)
      case e: java.io.IOException => 
        throw new ConfigurationException(s"Ошибка при чтении файла конфигурации: $inConfig - ${e.getMessage}", e)
      case e: Exception => 
        throw new ConfigurationException(s"Непредвиденная ошибка при чтении файла конфигурации: $inConfig", e)
    }
  }

  def getLines(inConfig: String, row: mutable.Map[String, String]): String = {
    try {
      val a = Files.readString(Paths.get(inConfig))
      return WorkWithText.replaceWithoutDecode(a, row)
    } catch {
      case e: java.nio.file.NoSuchFileException => 
        throw new ConfigurationException(s"Файл конфигурации не найден: $inConfig", e)
      case e: java.io.IOException => 
        throw new ConfigurationException(s"Ошибка при чтении файла конфигурации: $inConfig - ${e.getMessage}", e)
      case e: Exception => 
        throw new ConfigurationException(s"Непредвиденная ошибка при чтении или обработке файла конфигурации: $inConfig", e)
    }
  }
}