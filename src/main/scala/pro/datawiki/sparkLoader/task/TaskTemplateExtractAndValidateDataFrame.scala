package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.schemaValidator.SchemaValidator
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaObjectTemplate
import pro.datawiki.schemaValidator.projectSchema.SchemaObject
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass
import pro.datawiki.yamlConfiguration.YamlClass.toYaml

import java.nio.file.{Files, Paths}
import scala.collection.mutable

class TaskTemplateExtractAndValidateDataFrame(
                                               dataFrameIn: String,
                                               configLocation: String) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync: Boolean): List[DataFrameTrait] = {

    var df: DataFrame = SparkObject.spark.sql(s"""select * from $dataFrameIn""")
    LogMode.debugDF(df)
    val schema:BaseSchemaObjectTemplate = BaseSchemaObjectTemplate(df.schema.fields.toList)

    if configLocation == null then {
      throw Exception()
    }

    if !Files.exists(Paths.get(configLocation)) then {
      val yaml = YamlClass.toYaml(schema.getProjectSchema)
      YamlClass.writefile(configLocation, yaml)
    }

    val schemaSaved = SchemaObject(configLocation).getBaseSchemaTemplate

    if !SchemaValidator.validateDiffSchemas(schemaSaved,schema) then {
      throw Exception()
    }

    List[DataFrameTrait](DataFrameOriginal(df))
  }

}
