package pro.datawiki.diMigration.input.base

/**
 * Immutable case class для управления файлами миграции
 */
class LoadObject(processFilesList: List[String] = List.empty) {

  /**
   * Возвращает список пар (jobName, filePath)
   */
  def getList: List[String] = {
    return processFilesList
  }

  /**
   * Проверяет, есть ли файлы для обработки
   */
  def isEmpty: Boolean = processFilesList.isEmpty

  /**
   * Возвращает количество файлов для обработки
   */
  def size: Int = processFilesList.length
}

/**
 * Объект для обратной совместимости
 */
object LoadObject {
  def apply(): LoadObject = new LoadObject(List.empty)

  def apply(processFilesList: List[String]): LoadObject = new LoadObject(processFilesList)
}
