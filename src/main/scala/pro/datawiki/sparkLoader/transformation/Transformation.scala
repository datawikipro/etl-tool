package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{YamlConfigTransformation, YamlConfigTransformationTrait}

object Transformation {
  def run(objectName: String,transformation: YamlConfigTransformationTrait): Unit = {
    val df: DataFrame = transformation.getDataFrame
    df.createOrReplaceTempView(objectName)
  }

  def run(transformations: List[YamlConfigTransformation]): Unit = {
    transformations.foreach(i=> run(i.getObjectName,i.getTransformation))
  }
}
