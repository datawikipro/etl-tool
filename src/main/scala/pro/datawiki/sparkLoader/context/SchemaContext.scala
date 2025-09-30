package pro.datawiki.sparkLoader.context

import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate
import scala.collection.mutable

object SchemaContext {
  private val schemaCache: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

  /**
   * Получить схему из кэша по имени файла
   * @param fileName имя файла схемы
   * @return схема из кэша или null если не найдена
   */
  def getCachedSchema(fileName: String): Option[BaseSchemaTemplate] = {
    schemaCache.get(fileName)
  }

  /**
   * Сохранить схему в кэш
   * @param fileName имя файла схемы
   * @param schema схема для сохранения
   */
  def cacheSchema(fileName: String, schema: BaseSchemaTemplate): Unit = {
    schemaCache += (fileName -> schema)
  }

  /**
   * Проверить, есть ли схема в кэше
   * @param fileName имя файла схемы
   * @return true если схема есть в кэше
   */
  def isSchemaCached(fileName: String): Boolean = {
    schemaCache.contains(fileName)
  }

  /**
   * Очистить кэш схем
   */
  def clearCache(): Unit = {
    schemaCache.clear()
  }

  /**
   * Удалить конкретную схему из кэша
   * @param fileName имя файла схемы
   */
  def removeFromCache(fileName: String): Unit = {
    schemaCache.remove(fileName)
  }

  /**
   * Получить количество схем в кэше
   * @return количество схем
   */
  def getCacheSize(): Int = {
    schemaCache.size
  }
}
