package pro.datawiki.schemaValidator.spark

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaTemplate}
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElementRow
import pro.datawiki.schemaValidator.LogMode


object SparkConstructor {

  def getDataFrameFromListBaseSchemaObjects(in: List[BaseSchemaObject], inSchema: BaseSchemaObjectTemplate)(implicit spark: SparkSession): DataFrame = {
    if (inSchema == null) {
      throw new IllegalArgumentException("Schema cannot be null")
    }

    val df = try {
      val seqRow: List[Row] = in.map(col => inSchema.getSparkRowElement(col)).map(col => col.getRow)
      val rowsRDD = spark.sparkContext.parallelize(Seq.apply(seqRow *))
      val struct =  StructType(inSchema.inElements.map(i => StructField(i._1, i._2.getSparkRowElementTemplate.getType, true)))
      spark.createDataFrame(rowsRDD, struct)
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