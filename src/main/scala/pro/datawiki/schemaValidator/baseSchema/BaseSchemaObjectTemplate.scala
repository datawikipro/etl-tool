package pro.datawiki.schemaValidator.baseSchema

import org.apache.spark.sql.types.StructField
import pro.datawiki.exception.{NotImplementedException, SchemaValidationException}
import pro.datawiki.schemaValidator.dataFrame.DataFrameSchemaValidator
import pro.datawiki.schemaValidator.projectSchema.{SchemaElement, SchemaObject, SchemaType}
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElementRow
import pro.datawiki.schemaValidator.spark.sparkTypeTemplate.{SparkRowElementStructTemplate, SparkRowElementTypeTemplate}
import pro.datawiki.schemaValidator.spark.{SparkRowAttribute, SparkRowAttributeTemplate}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable
import scala.language.postfixOps

case class BaseSchemaObjectTemplate(inElements: List[(String, BaseSchemaTemplate)],
                                    inIsIgnorable: Boolean) extends BaseSchemaTemplate {
  def appendBaseSchemaTemplate(name: String, in: BaseSchemaTemplate): BaseSchemaObjectTemplate = {
    return BaseSchemaObjectTemplate(inElements ::: List((name, in)), inIsIgnorable)
  }

  private def checkExistsElementByName(in: String): Boolean = {
    inElements.filter(i => i._1 == in).foreach(col => return true)
    return false
  }

  def getElementByName(in: String): BaseSchemaTemplate = {
    inElements.filter(i => i._1 == in).foreach(col => return col._2)
    if inIsIgnorable then return BaseSchemaNullTemplate(inIsIgnorable)
    throw SchemaValidationException(s"Element '$in' not found in schema object")
  }

  private def getElementByNameSafe(in: String): Option[BaseSchemaTemplate] = {
    inElements.find(i => i._1 == in).map(_._2)
  }

  override def extractDataFromObject(in: BaseSchemaStruct): BaseSchemaStruct = {
    val inObject: BaseSchemaObject = in match
      case x: BaseSchemaObject => x
      case x: BaseSchemaNull => return BaseSchemaObject(List.apply(), inIsIgnorable)
      case _ => {
        throw NotImplementedException("Method not implemented")
      }

    return BaseSchemaObject(inElements.map(i => (i._1, i._2.extractDataFromObject(inObject.getElementStructByName(i._1, i._2)))), inIsIgnorable)
  }

  private def leftValidateElement(name: String, in: BaseSchemaTemplate, schema: BaseSchemaTemplate): Boolean = {
    val inSchema: BaseSchemaObjectTemplate = schema match {
      case x: BaseSchemaObjectTemplate => x
      case other => {
        throw Exception()
      }
    }
    val in2 = inSchema.getElementByName(name)
    return in.leftValidate(in2)

  }


  override def leftValidate(in: BaseSchemaTemplate): Boolean = {
    var newElements: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

    in match
      case x: BaseSchemaObjectTemplate => {
        val inElementNames = x.inElements.map(_._1).toSet
        val thisElementNames = inElements.map(_._1).toSet

        // Находим пересекающиеся поля
        val intersectingNames = inElementNames.intersect(thisElementNames)

        // Валидируем только пересекающиеся поля
        if !intersectingNames.forall(name => {
          val thisElement = this.getElementByNameSafe(name).get
          leftValidateElement(name, thisElement, x)
        }) then {
          return false
        }

        // Проверяем поля, которые есть в this, но нет в in
        val thisOnlyNames = thisElementNames -- inElementNames
        if !thisOnlyNames.forall(name => {
          val thisElement = this.getElementByNameSafe(name).get
          // Если поле отсутствует в in, оно должно быть игнорируемым
          thisElement.isIgnorable
        }) then {
          return false
        }

        return true

      }
      case x: BaseSchemaArrayTemplate => {
        throw NotImplementedException("Array template validation not implemented")
      }
      case x: BaseSchemaMapTemplate => {
        throw NotImplementedException("Map template validation not implemented")
      }
      case other => {
        throw SchemaValidationException(s"Невозможно извлечь данные объекта из: ${other.getClass.getName}")
      }
  }

  override def getSparkRowElementTemplate: SparkRowElementTypeTemplate = {
    try {
      val localList: List[SparkRowAttributeTemplate] =
        inElements.map(i => SparkRowAttributeTemplate(i._1, i._2.getSparkRowElementTemplate))
      return SparkRowElementStructTemplate(localList)
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def fullMerge(in: BaseSchemaTemplate): BaseSchemaTemplate = {
    var newElements: mutable.Map[String, BaseSchemaTemplate] = mutable.Map()

    in match
      case x: BaseSchemaObjectTemplate => {
        // Получаем все имена элементов из обоих объектов
        val thisElementNames = inElements.map(_._1).toSet
        val otherElementNames = x.inElements.map(_._1).toSet

        // Находим пересечения по именам
        val intersectingNames = thisElementNames.intersect(otherElementNames)

        // Обрабатываем пересекающиеся элементы - применяем fullMerge к их значениям
        intersectingNames.foreach(name => {
          val thisElement = this.getElementByNameSafe(name).get
          val otherElement = x.getElementByNameSafe(name).get
          newElements += (name, thisElement.fullMerge(otherElement))
        })

        // Добавляем элементы, которые не пересекаются
        val nonIntersectingThis = thisElementNames -- intersectingNames
        val nonIntersectingOther = otherElementNames -- intersectingNames

        nonIntersectingThis.foreach(name => {
          newElements += (name, this.getElementByNameSafe(name).get)
        })

        nonIntersectingOther.foreach(name => {
          newElements += (name, x.getElementByNameSafe(name).get)
        })

        return new BaseSchemaObjectTemplate(newElements.toList, inIsIgnorable)
      }
      case x: BaseSchemaNullTemplate => return new BaseSchemaObjectTemplate(inElements, inIsIgnorable)
      case x: BaseSchemaMapTemplate => {
        return new BaseSchemaMapTemplate(baseElement = x.baseElement, inIsIgnorable)
        //TODO
      }
      case other => {
        throw SchemaValidationException(s"Невозможно извлечь данные объекта из: ${other.getClass.getName}")
      }

  }

  override def getProjectSchema: SchemaObject = {
    val list: List[SchemaElement] = inElements.map(i => {
      i._2 match
        case x: BaseSchemaStringTemplate => (SchemaElement(name = i._1, `type` = SchemaType.String.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaNullTemplate => (SchemaElement(name = i._1, `type` = SchemaType.Null.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaObjectTemplate => (SchemaElement(name = i._1, `type` = null, `array` = null, `object` = x.getProjectSchema, `map` = null, isIgnorable = false))
        case x: BaseSchemaIntTemplate => (SchemaElement(name = i._1, `type` = SchemaType.Int.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaDoubleTemplate => (SchemaElement(name = i._1, `type` = SchemaType.Double.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaBooleanTemplate => (SchemaElement(name = i._1, `type` = SchemaType.Boolean.toString, `array` = null, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaArrayTemplate => (SchemaElement(name = i._1, `type` = null, `array` = x.getProjectSchema, `object` = null, `map` = null, isIgnorable = false))
        case x: BaseSchemaMapTemplate => (SchemaElement(name = i._1, `type` = null, `array` = null, `object` = null, `map` = x.getProjectSchema, isIgnorable = false))
        case _ => {
          throw NotImplementedException("Method not implemented")
        }
    })
    return SchemaObject(list)
  }

  override def isIgnorable: Boolean = inIsIgnorable

  override def equals(in: BaseSchemaTemplate): Boolean = {
    in match {
      case x: BaseSchemaObjectTemplate => {
        if (inElements.size != x.inElements.size) return false
        inElements.foreach(i => {
          if (!x.inElements.exists(_._1 == i._1)) then return false
          if (!x.inElements.find(_._1 == i._1).get._2.equals(i._2)) then return false
        })
        return true
      }
      case x: BaseSchemaNullTemplate => {
        return true
      }
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
    }

  }

  override def getSparkRowElement(data: BaseSchemaStruct): SparkRowElementRow = {
    data match {
      case x: BaseSchemaObject => {

        SparkRowElementRow(
          this.inElements.map(
            col =>
              SparkRowAttribute(
                col._1,
                col._2.getSparkRowElement(
                  x.getElementStructByName(col._1, col._2)
                )
              )
          )
        )
      }
      case fs => {
        throw SchemaValidationException(s"Unsupported data type for object template: ${fs.getClass.getName}")
      }
    }


  }
}


object BaseSchemaObjectTemplate {
  def apply(in: List[StructField]): BaseSchemaObjectTemplate = {
    val isIgnorable = false

    // Преобразуем каждое StructField в соответствующий BaseSchemaTemplate
    val templates: List[(String, BaseSchemaTemplate)] =
      in.map(field => (field.name, DataFrameSchemaValidator.convertStructFieldToTemplate(field)))

    val schema = new BaseSchemaObjectTemplate(templates, isIgnorable)
    val project = YamlClass.toYaml(schema.getProjectSchema)
    return schema
  }

  def apply(inElements: List[(String, BaseSchemaTemplate)],
            inIsIgnorable: Boolean): BaseSchemaObjectTemplate = {
    new BaseSchemaObjectTemplate(inElements, inIsIgnorable)
  }
}