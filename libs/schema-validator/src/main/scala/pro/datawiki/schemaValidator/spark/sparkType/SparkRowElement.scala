package pro.datawiki.schemaValidator.spark.sparkType

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.*
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.schemaValidator.baseSchema.BaseSchemaStruct
import pro.datawiki.schemaValidator.spark.SparkRowAttribute

trait SparkRowElement {
  def getValue: Any

  def getType: DataType

  def getBaseSchemaStruct: BaseSchemaStruct
}


object SparkRowElement {

  private def castBooleanType(in: Any): SparkRowElement = {
    in match {
      case y: Boolean => SparkRowElementBoolean(y)
      case y: Long => SparkRowElementBoolean(y == 1)
      case y: String => {
        y match {
          case "true" => SparkRowElementBoolean(true)
          case "false" => SparkRowElementBoolean(false)
          case "1" => SparkRowElementBoolean(true)
          case "" => SparkRowElementBoolean(false)
          case fs => {
            throw IllegalArgumentException(s"Invalid boolean string value: '$fs'")
          }
        }
      }
      case fs => {
        throw IllegalArgumentException(s"Unsupported type for boolean conversion: ${fs.getClass.getName}")
      }
    }
  }

  private def castLongType(in: Any): SparkRowElement = {
    in match {
      case z: Long => return SparkRowElementInt(z)
      case z: String => {
        z match {
          case "false" => {
            return SparkRowElementInt(0)
          }

          case "true" => {
            return SparkRowElementInt(1)
          }

          case fs => {
            try {
              return SparkRowElementInt(z.toLong)
            } catch {
              case e: Exception => {
                return return SparkRowElementInt(0)
                throw e
              }
            }
          }

        }

      }
      case fs => {
        println(fs.getClass)
        println(fs.getClass.getName)
        throw IllegalArgumentException(s"Unsupported type for long conversion: ${fs.getClass.getName}")
      }
    }
  }

  private def castDoubleType(in: Any): SparkRowElement = {
    in match {
      case y: Double => return SparkRowElementDouble(y)
      case y: Long => return SparkRowElementDouble(y.toDouble)
      case y: Int => return SparkRowElementDouble(y.toDouble)
      case fs => {
        throw IllegalArgumentException(s"Unsupported type for double conversion: ${fs.getClass.getName}")
      }
    }
  }

  private def castStringType(in: Any): SparkRowElement = {

    in match {
      case x: String => {
        return SparkRowElementString(x)
      }
      case x: Long => return SparkRowElementString(x.toString)
      case fs => {
        throw IllegalArgumentException(s"Unsupported type for string conversion: ${fs.getClass.getName}")
      }
    }
  }

  private def castStructType(inStruct: StructType, in: Any): SparkRowElement = {
    try {
      in match {
        case z: GenericRow => {
          val list: List[SparkRowAttribute] = inStruct.fields.zipWithIndex.map(
            (p, index) => {
              SparkRowAttribute(
                p.name,
                SparkRowElement(p.dataType, z.get(index))
              )
            }
          ).toList
          return SparkRowElementStruct(list)
        }
        case z: Long => {
          inStruct.fields.length match {
            case 1 => SparkRowElement(inStruct.fields.head.dataType, z)
            case _ => {
              throw IllegalArgumentException("Struct with multiple fields cannot be converted from single Long value")
            }
          }

        }
        case fs => {
          println(fs.getClass)
          println(fs.getClass.getName)
          throw IllegalArgumentException(s"Unsupported type for struct conversion: ${fs.getClass.getName}")
        }
      }
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  private def castArrayTypeNonRow(inArray: ArrayType, in: Any): SparkRowElement = {
    in match {
      case x: SparkRowElementString => return x
      case other => {
        throw IllegalArgumentException(s"Unsupported type for array element conversion: ${other.getClass.getName}")
      }
    }

  }

  private def castArrayTypeRow(inArray: ArrayType, in: Any): SparkRowElementRow = {

    in match {
      case x: SparkRowElementRow => return x
      case fs => {
        println(fs.getClass)
        throw IllegalArgumentException(s"Unsupported type for array row conversion: ${fs.getClass.getName}")
      }
    }

  }

  private def castArrayType(inArray: ArrayType, in: Any): SparkRowElement = {
    in match {
      case y: List[_] => {
        inArray.elementType match {
          case x: StructType => {
            val list: List[SparkRowElementRow] = y.map(col => castArrayTypeRow(inArray, col))
            return new SparkRowElementList(list, inArray)
          }
          case x: StringType => {
            val list: List[SparkRowElement] = y.map(col => castArrayTypeNonRow(inArray, col))
            return new SparkRowElementArray(list, inArray.elementType)
          }
          case fs => {
            println(fs.getClass)
            throw IllegalArgumentException(s"Unsupported array element type: ${fs.getClass.getName}")
          }
        }
      }
      case _ => {
        throw NotImplementedException("Array type conversion not implemented")
      }
    }
  }

  def apply(inType: DataType, in: Any): SparkRowElement = {
    if in == null then {
      throw IllegalArgumentException("Input value cannot be null")
    }

    inType match {
      case x: BooleanType => try {
        castBooleanType(in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case x: LongType => try {
        castLongType(in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case x: DoubleType => try {
        castDoubleType(in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case x: StringType => try {
        castStringType(in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case x: StructType => try {
        castStructType(x, in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case x: ArrayType => try {
        castArrayType(x, in)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      case fs => {
        throw NotImplementedException(s"Data type conversion not implemented: ${fs.getClass.getName}")
      }
    }
  }

}