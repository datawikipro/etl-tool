package pro.datawiki.schemaValidator.dataFrame

import org.apache.spark.sql.types.*
import pro.datawiki.schemaValidator.baseSchema.*
import pro.datawiki.schemaValidator.spark.sparkType.SparkRowElement

import scala.collection.mutable

object DataFrameSchemaValidator {

  def convertStructFieldToTemplate(field: StructField): BaseSchemaTemplate = {
    field.dataType match {
      case StringType => new BaseSchemaStringTemplate(field.nullable)
      case IntegerType => new BaseSchemaIntTemplate(field.nullable)
      case DoubleType => new BaseSchemaDoubleTemplate(field.nullable)
      case BooleanType => new BaseSchemaBooleanTemplate(field.nullable)
      case ArrayType(elementType, containsNull) => {
        // Создаем соответствующий шаблон для элементов массива
        val templateElement = elementType match {
          case StringType => new BaseSchemaStringTemplate(containsNull)
          case IntegerType => new BaseSchemaIntTemplate(containsNull)
          case DoubleType => new BaseSchemaDoubleTemplate(containsNull)
          case BooleanType => new BaseSchemaBooleanTemplate(containsNull)
          case _ => new BaseSchemaNullTemplate(containsNull)
        }
        // Создаем шаблон массива
        new BaseSchemaArrayTemplate(templateElement, field.nullable)
      }
      case StructType(fields) => {
        val subTemplates = fields.map(subField => (subField.name -> convertStructFieldToTemplate(subField))).toList

        new BaseSchemaObjectTemplate(subTemplates, field.nullable)
      }
      case _ => new BaseSchemaNullTemplate(field.nullable)
    }
  }
}
