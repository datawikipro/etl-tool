package pro.datawiki.schemaValidator.projectSchema

import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.context.SchemaContext
import pro.datawiki.yamlConfiguration.YamlClass

class ProjectSchemaConstructor extends Migration {
  override def extractStruct(inString: String): BaseSchemaStruct = {
    throw new NotImplementedException("extractStruct not implemented for ProjectSchemaConstructor")
  }

  override def readTemplate(inFileName: String): BaseSchemaTemplate = {
    // Проверяем кэш перед чтением файла
    SchemaContext.getCachedSchema(inFileName) match {
      case Some(cachedSchema) => return cachedSchema
      case None => // Продолжаем чтение файла
    }

    var resolvedPath = inFileName
    var configFile = java.nio.file.Paths.get(resolvedPath)
    if (!java.nio.file.Files.exists(configFile)) {
      val fallback = java.nio.file.Paths.get("configs", "pipelines", inFileName)
      if (java.nio.file.Files.exists(fallback)) {
        resolvedPath = fallback.toString
        configFile = fallback
      } else {
        return null
      }
    }

    val json = SchemaObject(resolvedPath)
    val schema = json.getBaseSchemaTemplate(false)
    
    // Сохраняем результат в кэш
    SchemaContext.cacheSchema(inFileName, schema)
    
    return schema
  }

  override def writeTemplate(baseSchema: BaseSchemaTemplate): String = {
    val schema = baseSchema.getProjectSchema

    return YamlClass.toYaml(schema)
  }

  override def readSchema(inFileName: String): BaseSchemaStruct = {
    throw new NotImplementedException("readSchema not implemented for ProjectSchemaConstructor")
  }

  override def writeSchema(inFileName: BaseSchemaStruct): String = ???

  override def extractSchema(inString: String): BaseSchemaTemplate = ???

  override def evolutionSchemaByObject(inString: String, inSchema: BaseSchemaTemplate): BaseSchemaTemplate = ???

  override def evolutionSchemaByList(inList: List[String], inSchema: BaseSchemaTemplate): BaseSchemaTemplate = ???

  override def getDataFrameFromString(inString: String, schemaName: String, inSchema: BaseSchemaTemplate, validData: Boolean,
                                      isStrongValidation: Boolean)(implicit spark: SparkSession): DataFrame = ???

  override def validateSchemaForObject(inString: String, inSchema: BaseSchemaTemplate): Boolean = ???
}
