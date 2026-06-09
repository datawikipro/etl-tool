package pro.datawiki.sparkLoader.connection.spark

import pro.datawiki.exception.{IllegalArgumentException, NotImplementedException}
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType

object LoaderSpark {

  def encodeDataType(in: TableMetadataType): String = {
    return in match {
      case _ => throw NotImplementedException(s"Spark data type encoding not implemented for: $in")
    }
  }

  def decodeDataType(in: String): TableMetadataType = {
    in match {
      case "long" => return TableMetadataType.Bigint
      case "timestamp" => return TableMetadataType.TimestampWithoutTimeZone
      case "date" => return TableMetadataType.Date
      case "string" => return TableMetadataType.String
      case "boolean" => return TableMetadataType.Boolean
      case "double" => return TableMetadataType.DoublePrecision
      case "integer" => return TableMetadataType.Integer
      case "array" => return TableMetadataType.Array
      case "decimal(20,6)" => return TableMetadataType.DoublePrecision
      case "decimal(20,0)" => return TableMetadataType.Integer
      case "decimal(20,5)" => return TableMetadataType.DoublePrecision
      case "float" => return TableMetadataType.DoublePrecision
      case "binary" => return TableMetadataType.String
      case _ => {
        throw NotImplementedException(s"Unsupported Spark data type decoding: '$in'")
      }
    }
  }
}
