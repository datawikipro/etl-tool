package pro.datawiki.sparkLoader.dictionaryEnum

enum SCDType {
  case SCD_0, SCD_1, SCD_2, SCD_3, SCD_UNDEFINED
}

object SCDType {
  /** Год-заглушка для актуальных записей SCD_2/SCD_3 ("бесконечная" дата) */
  val INFINITY_YEAR: String = "2100"

  /** Timestamp-строка для Spark withColumn (SCD_2/SCD_3 writeDf) */
  val INFINITY_TIMESTAMP: String = s"$INFINITY_YEAR-01-01 00:00:00"

  def apply(in: String): SCDType = {
    in match {
      case "SCD_0" | "scd0" | "scd_0" => SCD_0
      case "SCD_1" | "scd1" | "scd_1" => SCD_1
      case "SCD_2" | "scd2" | "scd_2" => SCD_2
      case "SCD_3" | "scd3" | "scd_3" => SCD_3
      case "SCD_UNDEFINED" | "undefined" => SCD_UNDEFINED
      case _ => throw Exception(s"Unsupported SCDType: $in")
    }
  }
}