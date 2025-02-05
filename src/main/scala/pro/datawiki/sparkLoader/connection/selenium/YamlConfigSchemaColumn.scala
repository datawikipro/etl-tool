package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.*

class YamlConfigSchemaColumn(
                              column: String,
                              `type`: String,
                              subType: List[YamlConfigSchemaColumn],
                              default: String,
                            ) {
  def getColumn:String = column
  def getType:String = `type`
  def getSubType:List[YamlConfigSchemaColumn] = subType
  def getDefault:String = default

//  private def getDefaultValue: SeleniumType = {
//    if default == "${now()}" then {
//      val currentDateTime: LocalDateTime = LocalDateTime.now()
//
//      val formatter: DateTimeFormatter = getDataType match
//        case DateType => DateTimeFormatter.ofPattern("yyyy-MM-dd")
//        case TimestampType => DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//        case StringType => DateTimeFormatter.ofPattern("yyyy-MM-dd")
//        case _ => throw Exception()
//      return SeleniumString(currentDateTime.format(formatter))
//    }
//
//    if default == "${seq}" then {
//      return SeleniumString(LoaderSelenium.getId.toString)
//    }
//    getDataType match
//      case x: StringType => return SeleniumString(default)
//      case x: IntegerType => return SeleniumString(default)
//      case x: DateType => return SeleniumString(default)
//      case x: ArrayType => return SeleniumArray.apply()
//      case _ => throw Exception()
//    throw Exception()
//  }

  def getStructField(in:SeleniumList): SparkRowAttribute = {
    var value = in.getValueByKey(column)

    `type` match
      case "string" => {
        val result: String = value match
          case null => default
          case x: SeleniumString => {
            val a = x.getValue
             a match
              case null => default
              case _ => a
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
              case "${now()}" =>{
                val currentDateTime: LocalDateTime = LocalDateTime.now()
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                val result = currentDateTime.format(formatter)
                return SparkRowAttribute(column, SparkRowElementString(result))
              }
              case _=> throw Exception()
          }
          case _ => throw Exception()
        }
      case "timestamp" =>        {
        value match
          case null => {
            default match
              case "${now()}" =>{
                val currentDateTime: LocalDateTime = LocalDateTime.now()
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                val result = currentDateTime.format(formatter)
                return SparkRowAttribute(column, SparkRowElementString(result))

              }
              case _=> throw Exception()

          }
          case _ => throw Exception()           }
      case "array" => {
        var subStruct: List[SparkRowAttribute] = List.apply()

        val result:SeleniumArray = value match
          case null => {
            val a: List[SeleniumList] = List.apply(SeleniumList.apply())
            new SeleniumArray(a)
          }
          case x: SeleniumArray => {
            x
          }
          case _ => {
            throw Exception()
          }
        var listSparkRowElementRow: List[SparkRowElementRow] = List.apply()
        result.list.foreach(j=> {
          var list1: List[SparkRowAttribute] = List.apply()
          subType.foreach(i => list1 = list1 :+ i.getStructField(j))
          listSparkRowElementRow = listSparkRowElementRow.appended(SparkRowElementRow(list1)) 
        })
        
        return SparkRowAttribute(column, SparkRowElementList(listSparkRowElementRow))
//        return SparkRowAttribute(column, SparkRowElementList(subStruct))
//        return StructField(name = column, dataType = ArrayType(StructType(subStruct)), nullable = false, metadata = Metadata.empty)
      }
      case "seq" => {
        val result: String = value match
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
      case _ => throw Exception()

  }

}