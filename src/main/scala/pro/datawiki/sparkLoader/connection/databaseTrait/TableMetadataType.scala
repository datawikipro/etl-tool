package pro.datawiki.sparkLoader.connection.databaseTrait

import pro.datawiki.exception.{IllegalArgumentException, NotImplementedException}
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres

enum TableMetadataType {
  case Integer, Uint, Bigint, String, Boolean, Varchar, Date,
  DoublePrecision, Numeric, Real, Text,
  TimestampWithTimeZone, TimestampWithoutTimeZone, Array

  def getTypeInSystem(in: String): String = {

    in match {
      case "postgres" => return LoaderPostgres.encodeDataType(this)
      case _ => throw IllegalArgumentException("Unsupported TableMetadataType")
    }

  }

  def getMasterType: TableMetadataType = {
    this match {
      case Integer => return Integer
      case Uint => Integer
      case Bigint => Integer
      case String => return String
      case Boolean => return Boolean
      case Varchar => return String
      case Date => return Date
      case DoublePrecision => return Real
      case Numeric => return Real
      case Real => return Real
      case Text => return String
      case Array => return String //TODO Срочно
      case TimestampWithTimeZone => return Date
      case TimestampWithoutTimeZone => return Date
      case _ => {
        throw NotImplementedException(s"getMasterType not implemented for: ${this}")
      }
    }
  }
}