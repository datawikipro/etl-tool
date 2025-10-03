package pro.datawiki.diMigration.input.base

/**
 * Immutable case class для управления файлами миграции
 */
class LoadObject(processFilesList: List[String] = List.empty) {

  def getList: List[String] = {
    return processFilesList
  }

  def isEmpty: Boolean = processFilesList.isEmpty

  def size: Int = processFilesList.length
}

/**
 * Объект для обратной совместимости
 */
object LoadObject {
  def apply(): LoadObject = new LoadObject(List.empty)

  def apply(processFilesList: List[String]): LoadObject = new LoadObject(processFilesList)
}
