package pro.datawiki.schemaValidator

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.{JObject, JValue}
import pro.datawiki.sparkLoader.LogMode

import scala.collection.mutable

class BaseSchemaObjectTemplate(inElements: mutable.Map[String, BaseSchemaTemplate],
                               inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  def getElementByName(in: String):BaseSchemaTemplate ={
    val b = inElements.get(in)
    if b == null then return BaseSchemaNullTemplate(inIsIgnorable)
    return b.get
  }

  override def getTemplate: BaseSchemaTemplate = this

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    var newElements: mutable.Map[String, BaseSchemaStruct] = mutable.Map()

    val inObject:BaseSchemaObject = in match
      case x: BaseSchemaObject => x
      case _ => throw Exception()

    inElements.foreach(i => {
      newElements += (i._1, i._2.extractDataFromObject(inObject.getElementStructByName(i._1)))
    })
    return BaseSchemaObject(newElements,inElements,inIsIgnorable)
  }

  override def leftMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    var newElements: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

    val inObject = in match
      case x: BaseSchemaObjectTemplate => x
      case _ => throw Exception()

    inElements.foreach(i => {
      newElements += (i._1, i._2.fullMerge(inObject.getElementByName(i._1)))
    })
    return BaseSchemaObjectTemplate(newElements, inIsIgnorable)
  }

}