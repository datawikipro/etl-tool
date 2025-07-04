package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.schemaValidator.sparkRow.{SparkRowAttribute, SparkRowElementList, SparkRowElementRow, SparkRowElementString}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

class YamlConfigSchemaColumn(
                              column: String,
                              `type`: String,
                              subType: List[YamlConfigSchemaColumn],
                              default: String,
                            ) {
  def getColumn: String = column

  def getType: String = `type`

  def getSubType: List[YamlConfigSchemaColumn] = subType

  def getDefault: String = default

  def getStructField(in: Map[String, SeleniumType]): SparkRowAttribute = {
    var value = in.get(column)

    `type` match
      case "string" => {
        val result: String = value.isEmpty match
          case true => default
          case false => {
            val a = value.get.getValue
            a match
              case null => default
              case x: String => x
          }
          case _ => throw Exception()

        return SparkRowAttribute(column, SparkRowElementString(result))

      }
      case "integer" => {
        throw Exception()
        //return StructField(name = column, dataType = StringType, nullable = false, metadata = Metadata.empty) //IntegerType
      }
      case "date" => {
        value match
          case null => {
            default match
              case "${now()}" => {
                val currentDateTime: LocalDateTime = LocalDateTime.now()
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                val result = currentDateTime.format(formatter)
                return SparkRowAttribute(column, SparkRowElementString(result))
              }
              case _ => throw Exception()
          }
          case _ => throw Exception()
      }
      case "timestamp" => {
        value match
          case null => {
            default match
              case "${now()}" => {
                val currentDateTime: LocalDateTime = LocalDateTime.now()
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                val result = currentDateTime.format(formatter)
                return SparkRowAttribute(column, SparkRowElementString(result))

              }
              case _ => throw Exception()

          }
          case _ => throw Exception()
      }
      case "array" => {
        var subStruct: List[SparkRowAttribute] = List.apply()

        val result: SeleniumArray = value.isEmpty match
          case true => SeleniumArray(List.apply())
          case false => {
            value.get match
              case x: SeleniumArray => {
                x
              }
              case _ => {
                throw Exception()
              }
          }
          case _ => {
            throw Exception()
          }
        var listSparkRowElementRow: List[SparkRowElementRow] = List.apply()

        result.getList.foreach(j => {
          var list1: List[SparkRowAttribute] = List.apply()
          subType.foreach(i => list1 = list1 :+ i.getStructField(j))
          listSparkRowElementRow = listSparkRowElementRow.appended(SparkRowElementRow(list1))
        })

        return SparkRowAttribute(column, SparkRowElementList(listSparkRowElementRow))
      }
      case "seq" => {
        val result: String = value.get match
          case null => {
            default match
              case "${seq}" => LoaderSelenium.getId.toString
              case _ => throw Exception()
          }
          case x: SeleniumString => x.getValue
          case _ => throw Exception()

        return SparkRowAttribute(column, SparkRowElementString(result))
        throw Exception()
        //return StructField(name = column, dataType = StringType, nullable = false, metadata = Metadata.empty)
      }
      case _ =>
        throw Exception()

  }

  def getModified(parameters: mutable.Map[String, String]):YamlConfigSchemaColumn={
    getDefault match
      case null => return YamlConfigSchemaColumn(column = getColumn, `type` = getType, subType = getSubType, default = getDefault)
      case _ => return YamlConfigSchemaColumn(column = getColumn, `type` = getType, subType = getSubType, default = YamlConfig.getModifiedString(getDefault,parameters))
  }
  
  
}