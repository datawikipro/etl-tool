package pro.datawiki.sparkLoader.connection.databaseTrait

enum TableMetadataType {
  case Integer, Bigint, String, Boolean, Varchar, Date,
       DoublePrecision,Numeric, Real, Text,
       TimestampWithTimeZone, TimestampWithoutTimeZone
}