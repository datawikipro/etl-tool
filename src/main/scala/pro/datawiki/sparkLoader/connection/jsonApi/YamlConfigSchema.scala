package pro.datawiki.sparkLoader.connection.jsonApi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.file.DataFileWriter
import org.json4s.*
import org.json4s.jackson.JsonMethods.*
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameTrait}
import pro.datawiki.schemaValidator.SchemaValidator
import org.apache.spark.sql
import pro.datawiki.sparkLoader.LogMode

import java.io.File
import java.nio.file.{Files, Paths}

case class YamlConfigSchema(
                             schemaName: String,
                             fileLocation: String,
                             isError: Boolean
                           ) {
  def getSchemaByJson(jsonString: String): DataFrameTrait = {
    if LogMode.isDebug then {
      println("------------------------------------------------------------------------------------------")
      println(jsonString)
      println("------------------------------------------------------------------------------------------")
    }
    if isError then {
      try {
        val df = SchemaValidator.getDataFrameFromJson(jsonString, fileLocation)
        return DataFrameDirty(schemaName, df, false)
      } catch
        case _ => return null
    }
    val df = SchemaValidator.getDataFrameFromJson(jsonString, fileLocation)
    return DataFrameDirty(schemaName, df, true)

  }
}