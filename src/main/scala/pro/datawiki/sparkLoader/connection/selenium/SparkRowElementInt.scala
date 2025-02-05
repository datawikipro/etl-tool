package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.types.DataType

class SparkRowElementInt(in:Int) extends SparkRowElementType{
  override def getValue:Any = throw Exception()
  override def getType: DataType = throw Exception()
}
