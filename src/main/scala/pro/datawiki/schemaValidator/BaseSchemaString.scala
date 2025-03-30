package pro.datawiki.schemaValidator

import pro.datawiki.sparkLoader.LogMode

class BaseSchemaString(in: String,
                            inIsIgnorable:Boolean) extends BaseSchemaStringTemplate(inIsIgnorable), BaseSchemaStruct {
  def getValue:String = in

  def getSparkRowElement: SparkRowElementType = SparkRowElementString(in)
  override def isIgnorable:Boolean = inIsIgnorable
}
