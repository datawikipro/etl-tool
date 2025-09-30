package pro.datawiki.diMigration.interfaces

/**
 * Интерфейс для работы с файловой системой
 * Позволяет легко мокировать для тестов
 */
trait FileSystemService {
  def listFiles(dir: String): List[String]

  def listFilesWithPostfix(dir: String, postfix: String): List[String]

  def readFile(path: String): String

  def writeFile(path: String, content: String): Unit

  def fileExists(path: String): Boolean

  def isDirectory(path: String): Boolean
}