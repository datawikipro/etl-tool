package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationExtractJsonWithoutSchema.YamlConfigTransformationExtractJsonWithoutSchemaColumn


case class YamlConfigTransformationExtractJsonWithoutSchema(
                                                             inTable: String,
                                                             columns: List[YamlConfigTransformationExtractJsonWithoutSchemaColumn]
                                                           ) extends YamlConfigTransformationTrait {
  private def checkAddNewColumn(field: StructField): Unit = {
    columns.foreach(j => {
      if j.getColumnName == field.name then {
        return
      }
    })
    throw Exception()
  }

  private def checkAddNewColumns(fields: List[StructField]): Unit = {
    fields.foreach(checkAddNewColumn)
  }

  private def existColumnInFields(column: YamlConfigTransformationExtractJsonWithoutSchemaColumn, fields: List[StructField]): StructField = {
    fields.foreach(field => {
      if column.getColumnName == field.name then {
        return field
      }
    })
    return null
  }

  private def getSelectColumn(prevLevel: String, column: YamlConfigTransformationExtractJsonWithoutSchemaColumn, fields: List[StructField]): List[Column] = {
    column.getAction match
      case "extract" =>
        column.getColumnType match
          case "struct" =>
            var list: List[Column] = List.apply()
            val field = existColumnInFields(column, fields)
            if field == null then {
              column.getSubColumns.foreach(i => list = list ::: getSelectColumn(prevLevel + column.getColumnName + ".", i, List.apply()))
            } else {
              field.dataType match
                case x: StructType =>
                  column.getSubColumns.foreach(i => list = list ::: getSelectColumn(prevLevel + column.getColumnName + ".", i, x.fields.toList))
                case _ =>
                  throw Exception()
            }
            return list
          case "string" | "long" | "boolean" | "double" => {
            if existColumnInFields(column, fields) != null then {
              return List.apply(col(prevLevel + column.getColumnName).as(column.getNewColumnName))
            }
            else {
              return List.apply(lit(null).cast(column.getColumnType).as(column.getColumnName).as(column.getNewColumnName))
            }
          }

          case _ =>
            throw Exception()
      case "extractAny" => {
        if existColumnInFields(column, fields) != null then {
          return List.apply(col(prevLevel + s"${column.getColumnName}.*"))
        }
        else {
          return List.apply()
        }
      }
      case "ignore" =>
        if existColumnInFields(column, fields) != null then {
          return List.apply(col(prevLevel + s"${column.getColumnName}"))
        }
        else {
          return List.apply()
        }
      case _ =>
        throw Exception()
  }

  override def getDataFrame: DataFrame = {
    val df = SparkObject.spark.table(inTable)
    val schema = df.schema
    checkAddNewColumns(df.schema.fields.toList)

    var selectedColumns: List[Column] = List.apply()
    columns.foreach(i => selectedColumns = selectedColumns ::: getSelectColumn("", i, df.schema.fields.toList))

    val fields: Array[StructField] = schema.fields
    val df2 = df.select(selectedColumns*)
//    df2.printSchema()
//    df2.show()
    return df2
  }

}
