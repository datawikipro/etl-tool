package pro.datawiki.schemaValidator.spark

import org.apache.spark.sql.Row

case class SparkRowElementRowTemplate(in: List[SparkRowAttributeTemplate]) {
  def getSparkRowListAttributeTemplate: List[SparkRowAttributeTemplate] = in

}
