package pro.datawiki.schemaValidator.projectSchema

import pro.datawiki.exception.IllegalArgumentException

enum SchemaType {
  case Object, Array, String, Int, Boolean, Null, Long, Double

  override def toString: String = {
    this match {
      case Object => "Object"
      case Array => "Array"
      case String => "String"
      case Int => "Int"
      case Long => "Long"
      case Boolean => "Boolean"
      case Null => "Null"
      case Double => "Double"
    }
  }

  /**
   * Проверяет, является ли тип числовым
   */
  def isNumeric: Boolean = this match {
    case Int | Long | Double => true
    case _ => false
  }

  /**
   * Проверяет, является ли тип примитивным
   */
  def isPrimitive: Boolean = this match {
    case String | Int | Long | Boolean | Double | Null => true
    case _ => false
  }

  /**
   * Проверяет, является ли тип составным
   */
  def isComplex: Boolean = this match {
    case Object | Array => true
    case _ => false
  }
}

object SchemaType {
  /**
   * Создает SchemaType из строки
   */
  def apply(typeStr: String): SchemaType = {
    typeStr.toLowerCase match {
      case "string" => SchemaType.String
      case "int" => SchemaType.Int
      case "long" => SchemaType.Long
      case "boolean" => SchemaType.Boolean
      case "double" => SchemaType.Double
      case "null" => SchemaType.Null
      case "object" => SchemaType.Object
      case "array" => SchemaType.Array
      case _ => throw IllegalArgumentException(s"Unsupported schema type: $typeStr")
    }
  }
}
