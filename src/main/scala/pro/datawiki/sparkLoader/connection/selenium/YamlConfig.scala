package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, Metadata, StructField, StructType}

class YamlConfig(
                 url: String,
                 actions: List[Any],
                 schema: List[YamlConfigSchemaColumn],
                 template: List[YamlConfigTemplate]
               ) {
  def copy:YamlConfig = {
    new YamlConfig(url=url,      actions= actions,      schema= schema,      template= template)
  }
  
  var modifiedSchema:List[YamlConfigSchemaColumn] = schema
  var modifiedUrl:String = url

  def getUrl:String = modifiedUrl
  def getTemplate: List[YamlConfigTemplate] = template

  def modifyConfig(key:String, value: String): Unit = {
    modifiedUrl = modifiedUrl.replace("""${""" + key + """}""", value)
    var newSchema:List[YamlConfigSchemaColumn] = List.apply()
    modifiedSchema.foreach(i=> {
      i.default match
        case null => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.column, `type` = i.`type`,subType = i.subType, default = i.default)
        case _ => newSchema = newSchema :+ YamlConfigSchemaColumn(column = i.column, `type` = i.`type`,subType = i.subType, default = i.default.replace("""${""" + key + """}""", value))
    })
    modifiedSchema = newSchema
  }

  def convertToSchema(in: List[KeyValue]): Row = {
    var lst: List[Any] = List.apply()
    if modifiedSchema == null then {
      throw Exception()
    }
    modifiedSchema.foreach(i => {
      lst = lst :+ i.getColumnByListKeyValue(in)
    })
    val tst = Row.apply(lst: _*)

    return tst
  }

  def getSchema: StructType = {
    var lst: List[StructField] = List.apply()
    modifiedSchema.foreach(i => {
      lst = lst :+ i.getStructField
    })

    val sch = StructType(lst)

    return sch
  }
}
