package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.{YamlConfigTransformation, YamlConfigTransformationTrait}
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

object Transformation {
  def run(objectName: String,transformation: YamlConfigTransformationTrait): Unit = {
    val df: DataFrame = transformation.getDataFrame()

    df.createTempView(objectName)
  }

  def run(transformations: List[YamlConfigTransformation]): Unit = {
    if transformations == null then return

    transformations.foreach(i=> run(i.getObjectName,i.getTransformation))
  }
}
