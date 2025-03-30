package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}

class YamlConfig(
                  url: String,
                  schema: List[YamlConfigSchemaColumn] = List.apply(),
                  template: List[YamlConfigTemplate] = List.apply()
                ) {
  def getUrl: String = url

  def getSchema: List[YamlConfigSchemaColumn] = schema

  def getTemplate: List[YamlConfigTemplate] = template
}


object YamlConfig {
  def apply(in: YamlConfig,row: Row): YamlConfig = {
    if row == null then return in
    
    var modifiedSchema: List[YamlConfigSchemaColumn] = in.getSchema
    var modifiedUrl: String = in.getUrl
        
    row.schema.fields.foreach(j => {
      val key = j.name
      val value = row.get(row.fieldIndex(j.name)).toString
      modifiedUrl = modifiedUrl.replace("""${""" + key + """}""", value)
      var newSchema: List[YamlConfigSchemaColumn] = List.apply()
      modifiedSchema.foreach(i => {
        i.getDefault match
          case null => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.getColumn, `type` = i.getType, subType = i.getSubType, default = i.getDefault)
          case _ => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.getColumn, `type` = i.getType, subType = i.getSubType, default = i.getDefault.replace("""${""" + key + """}""", value))
      })
      modifiedSchema = newSchema
    })
    
    return new YamlConfig(url = modifiedUrl, schema = modifiedSchema, template = in.getTemplate)


  }

  def modifyConfig(modifiedUrl: String,modifiedSchema: List[YamlConfigSchemaColumn],key: String, value: String): Unit = {

  }
}