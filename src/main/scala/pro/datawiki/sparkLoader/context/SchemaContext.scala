package pro.datawiki.sparkLoader.context

import pro.datawiki.schemaValidator.baseSchema.BaseSchemaTemplate
import scala.collection.mutable

object SchemaContext {
  private val schemaCache: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

  def getCachedSchema(fileName: String): Option[BaseSchemaTemplate] = {
    schemaCache.get(fileName)
  }

  def cacheSchema(fileName: String, schema: BaseSchemaTemplate): Unit = {
    schemaCache += (fileName -> schema)
  }

  def isSchemaCached(fileName: String): Boolean = {
    schemaCache.contains(fileName)
  }

  def clearCache(): Unit = {
    schemaCache.clear()
  }

  def removeFromCache(fileName: String): Unit = {
    schemaCache.remove(fileName)
  }

  def getCacheSize(): Int = {
    schemaCache.size
  }
}
