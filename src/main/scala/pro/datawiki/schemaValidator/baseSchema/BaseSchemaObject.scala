package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import pro.datawiki.schemaValidator.sparkRow.{SparkRow, SparkRowAttribute, SparkRowElementStruct, SparkRowElementType}

import scala.collection.mutable

class BaseSchemaObject(inElements: mutable.Map[String, BaseSchemaStruct],
                       inTemplates: mutable.Map[String, BaseSchemaTemplate],
                       inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaObjectTemplate(inTemplates, inIsIgnorable)
  def getElements: mutable.Map[String, BaseSchemaStruct] = inElements

  def getElementStructByName(in: String): BaseSchemaStruct = {
    val b = inElements.get(in)
    if b == null then return BaseSchemaNull(inIsIgnorable)
    if b.isEmpty then return BaseSchemaNull(inIsIgnorable)
    return b.get
  }
  
  def packageDataFrame: DataFrame = {
    var attributes: List[SparkRowAttribute] = List.apply()

    getElements.foreach(i => {
      attributes = attributes.appended(SparkRowAttribute(i._1, i._2.getSparkRowElement))
    })
    val sparkRow: SparkRow = SparkRow(attributes)
    return sparkRow.getDataFrame
  }


  //
  //  def checkElementExists(inName: String, inType: BaseSchemaStruct): Boolean = {
  //    inElements.get(inName) match
  //      case Some(res) => return res.equals(inType)
  //
  //      case _ => return LogMode.getDebugFalse
  //  }
  //
  //  override def equals(in: BaseSchemaStruct): Boolean = {
  //    in match
  //      case x: BaseSchemaObject => {
  //        //TODO
  //        //if x.getElements.size != inElements.size then return LogMode.getDebugFalse
  //        val schemaElements = x.getElements
  //        schemaElements.foreach(i => {
  //          inElements.get(i._1) match
  //            case Some(elem) => {
  //              if !i._2.equals(elem) then return LogMode.getDebugFalse
  //            }
  //            case _ => {}
  //        })
  //        inElements.foreach(i => {
  //          schemaElements.get(i._1) match
  //            case Some(elem) => {
  //              if !i._2.equals(elem) then return LogMode.getDebugFalse
  //            }
  //            case _ => {
  //              if !i._2.isIgnorable then return LogMode.getDebugFalse
  //            }
  //        })
  //
  //        return true
  //      }
  //      case x: BaseSchemaMapTemplate => {
  //        inElements.foreach(i => {
  //          if !i._2.equals(x.getSubElement) then return LogMode.getDebugFalse
  //        })
  //        return true
  //      }
  //      case _ => return LogMode.getDebugFalse
  //  }
  //
  //  override def mergeSchema(schemaObject: BaseSchemaElement): BaseSchemaElement = {
  //    throw Exception()
  ////    schemaObject match
  ////      case x: BaseSchemaObject => {
  ////        var localElements: mutable.Map[String, BaseSchemaElement] = mutable.Map()
  ////        var parameterElements: mutable.Map[String, BaseSchemaElement] = x.getElements
  ////
  ////
  ////        inElements.foreach(i => {
  ////          parameterElements.get(i._1) match
  ////            case Some(x) => localElements += (i._1, i._2.mergeSchema(x))
  ////            case None =>
  ////            case _ => throw Exception()
  ////        })
  ////        return BaseSchemaObject(parameterElements,inIsIgnorable)
  ////      }
  ////      case _ => throw Exception()
  //  }

  override def getSparkRowElement: SparkRowElementType = {
    var localList: List[SparkRowAttribute] = List.apply()
    inElements.foreach(i => {
      localList = localList.appended(SparkRowAttribute(i._1, i._2.getSparkRowElement))
    })
    return SparkRowElementStruct(localList)
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}
