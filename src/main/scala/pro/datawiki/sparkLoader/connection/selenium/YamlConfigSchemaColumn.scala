package pro.datawiki.sparkLoader.connection.selenium

import org.apache.spark.sql.types.{ArrayType, DataType, DateType, IntegerType, Metadata, StringType, StructField, StructType, TimestampType}
import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import org.apache.spark.sql.{Row}

import java.time.Duration
import scala.jdk.CollectionConverters.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class YamlConfigSchemaColumn(
                                   column: String,
                                   `type`: String,
                                   subType: List[YamlConfigSchemaColumn],
                                   default: String,
                                 ) {
  def getDefault: String = {
    if default == "${now()}" then {
      val currentDateTime: LocalDateTime = LocalDateTime.now()

      val formatter: DateTimeFormatter = getType match
        case DateType => DateTimeFormatter.ofPattern("yyyy-MM-dd")
        case TimestampType => DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        case StringType => DateTimeFormatter.ofPattern("yyyy-MM-dd")
        case _ => throw Exception()
      return currentDateTime.format(formatter)
    }

    if default == "${seq}" then {
      return LoaderSelenium.getId.toString
    }
    getType match
      case x: StringType => return default
      case x: IntegerType => return default
      case x: DateType => return default
      //case x: DatetimeType => return default
      case _ => throw Exception()
    throw Exception()
  }

  def getType: DataType = {
    `type` match
      case "string" => return StringType
      case "integer" => return StringType //IntegerType
      case "date" => return StringType //DateType
      case "timestamp" => StringType //TimestampType
      case "array" =>
        var subStruct: List[StructField] = List.apply()
        subType.foreach(i => subStruct = subStruct :+ i.getStructField)
        return ArrayType(StructType(subStruct))
      case "seq" => return IntegerType
      case _ => throw Exception()
  }

  def getStructField: StructField = {
    return StructField(name = column, dataType = getType, nullable = false, metadata = Metadata.empty)
  }

  def getColumnByKeyValue(in: KeyValue): Any = {
    if in.key != column then {
      return null
    }
    if in.value == null then {
      return getDefault
    }

    getType match
      case x: ArrayType =>
        var list: List[Row] = List.apply()
        in.value match
          case y: List[List[KeyValue]] => {
            y.foreach(z => {
              var list2: List[Any] = List.apply()
              subType.foreach(j => {
                list2 = list2 :+ j.getColumnByListKeyValue(z)
              })
              list = list :+ Row.apply(list2: _*)
            })
          }
          case _ => throw Exception()
        return list
      case _ => return in.value
  }

  def getColumnByListKeyValue(in: List[KeyValue]): Any = {
    in.foreach(i => {
      val a = getColumnByKeyValue(i)
      if a != null then {
        return a
      }
    })
    getType match
      case x: ArrayType => return List.apply()
      case _ => getDefault
  }

}