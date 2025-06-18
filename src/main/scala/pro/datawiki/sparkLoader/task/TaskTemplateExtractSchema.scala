package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.functions.*
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.schemaValidator.SchemaValidator
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateExtractSchema(tableName: String,
                                jsonColumn: String,
                                jsonResultColumn: String,
                                baseSchema: String) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    var df = SparkObject.spark.sql(s"""select * from $tableName""")
    LogMode.debugDF(df)
    val list = df.filter(s"$jsonColumn <> '' and $jsonColumn is not null").select(s"$jsonColumn").distinct().collect().toList
    var jsons: List[String] = List.apply()
    list.foreach(i => {
      jsons = jsons.appended(i.get(0).toString)
    })
    val jsonTemplate = baseSchema match
      case null => SchemaValidator.getSchemaFromJson(jsons)
      case _ => SchemaValidator.validateListJsonByTemplateAndGetDataType(jsons,baseSchema, false)

    df = df.withColumn(jsonResultColumn, from_json(col(jsonColumn), jsonTemplate))
    LogMode.debugDF(df)
    return List.apply(DataFrameOriginal(df))
  }

}
