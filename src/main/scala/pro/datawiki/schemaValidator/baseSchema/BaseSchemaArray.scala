package pro.datawiki.schemaValidator.baseSchema

import pro.datawiki.schemaValidator.sparkRow.*

class BaseSchemaArray(list: List[BaseSchemaStruct],
                      baseElement: BaseSchemaTemplate,
                      inIsIgnorable: Boolean) extends BaseSchemaStruct {
  val loc = BaseSchemaArrayTemplate(baseElement, inIsIgnorable)

  def getList: List[BaseSchemaStruct] = list

  def getSubElement: BaseSchemaTemplate = baseElement

  override def getSparkRowElement: SparkRowElementList = {
    baseElement match {
      case y: BaseSchemaStringTemplate => {
        var l: List[SparkRowElementRow] = List.apply()
        list.foreach(i => {
          i.getSparkRowElement match
            case x: SparkRowElementString => {
              l = l.appended(SparkRowElementRow(
                List.apply(SparkRowAttribute(name = "data", value = x
                ))))
            }
            case _ =>
              throw Exception()
        })
        return SparkRowElementList(l)
      }

      case y: BaseSchemaObjectTemplate => {
        var l: List[SparkRowElementRow] = List.apply()
        list.foreach(i => {
          i.getSparkRowElement match
            case x: SparkRowElementStruct => {
              l = l.appended(x.getRow)
            }
            case _ =>
              throw Exception()
        })

        return SparkRowElementList(l.toSeq)
      }
      case _ => {
        throw Exception()
      }
    }

  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def getTemplate: BaseSchemaTemplate = loc
}

object BaseSchemaArray {
  def applyStruct(list: List[BaseSchemaStruct],
                  baseElement: BaseSchemaTemplate,
                  inIsIgnorable: Boolean): BaseSchemaStruct = {
    return new BaseSchemaArray(list, baseElement, inIsIgnorable)
  }

  def applyTemplate(list: List[BaseSchemaStruct],
                    baseElement: BaseSchemaTemplate,
                    inIsIgnorable: Boolean): BaseSchemaTemplate = {
    return new BaseSchemaArrayTemplate(baseElement, inIsIgnorable)
  }


}