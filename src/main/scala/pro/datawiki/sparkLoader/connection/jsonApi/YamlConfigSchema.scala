package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.file.DataFileWriter
import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.schemaValidator.SchemaValidator

import java.io.File
import java.nio.file.{Files, Paths}

case class YamlConfigSchema(
                             schemaName: String,
                             fileLocation: String
                           ) {
  def getSchemaByDataFrame(df:String): String={
    if SchemaValidator.checkJson(df,fileLocation) then return schemaName
    return null
  }
}