package pro.datawiki.schemaValidator.projectSchema

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.Migration
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaStruct, BaseSchemaTemplate}
import pro.datawiki.sparkLoader.context.SchemaContext
import pro.datawiki.yamlConfiguration.YamlClass
import pro.datawiki.yamlConfiguration.YamlClass.toYaml

class ProjectSchemaConstructor extends Migration {
  override def extractStruct(inString: String): BaseSchemaStruct = {
    throw NotImplementedException("extractStruct not implemented for ProjectSchemaConstructor")
  }

  override def readTemplate(inFileName: String): BaseSchemaTemplate = {
    // Проверяем кэш перед чтением файла
    SchemaContext.getCachedSchema(inFileName) match {
      case Some(cachedSchema) => return cachedSchema
      case None => // Продолжаем чтение файла
    }

    val configFile = java.nio.file.Paths.get(inFileName)
    if (!java.nio.file.Files.exists(configFile)) {
      return null
    }

    val json = SchemaObject(inFileName)
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
    throw NotImplementedException("readSchema not implemented for ProjectSchemaConstructor")
  }

  override def writeSchema(inFileName: BaseSchemaStruct): String = ???

  override def extractSchema(inString: String): BaseSchemaTemplate = ???

  override def evolutionSchemaByObject(inString: String, inSchema: BaseSchemaTemplate): BaseSchemaTemplate = ???

  override def evolutionSchemaByList(inList: List[String], inSchema: BaseSchemaTemplate): BaseSchemaTemplate = ???

  override def getDataFrameFromString(inString: String,schemaName:String,  inSchema: BaseSchemaTemplate, validData: Boolean,
                                      isStrongValidation: Boolean): DataFrameTrait = ???

  override def validateSchemaForObject(inString: String, inSchema: BaseSchemaTemplate): Boolean = ???
}
