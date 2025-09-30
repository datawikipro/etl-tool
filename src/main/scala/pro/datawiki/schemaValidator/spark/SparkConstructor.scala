package pro.datawiki.schemaValidator.spark

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElementRow
import pro.datawiki.sparkLoader.{LogMode, SparkObject}


object SparkConstructor {

  def getDataFrameFromListBaseSchemaObjects(in: List[BaseSchemaObject], inSchema: BaseSchemaObjectTemplate): DataFrame = {
    if inSchema == null then {
      throw IllegalArgumentException("Schema cannot be null")
    }

    val df = try {
      val seqRow: List[Row] = in.map(col => inSchema.getSparkRowElement(col)).map(col => col.getRow)
      val rowsRDD = SparkObject.spark.sparkContext.parallelize(Seq.apply(seqRow *))
      val struct =  StructType(inSchema.inElements.map(i => StructField(i._1, i._2.getSparkRowElementTemplate.getType, true)))
      SparkObject.spark.createDataFrame(rowsRDD, struct)
    } catch {
      case e: Exception => {
        throw e
      }
    }
    LogMode.debugDF(df)
    return df
  }

  def getBaseSchemaTemplate(in: StructType): BaseSchemaObjectTemplate = {
    return BaseSchemaObjectTemplate(in.toList)
  }
}