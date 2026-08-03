package pro.datawiki.sparkLoader.dictionaryEnum

enum SCDType {
  case SCD_0, SCD_1, SCD_2, SCD_3, SCD_UNDEFINED
}

object SCDType {
  def apply(in: String): SCDType = {
    if (in == null) return SCD_UNDEFINED
    in.trim.toUpperCase.replace("-", "_") match {
      case "SCD_0" | "SCD0" | "0" => SCD_0
      case "SCD_1" | "SCD1" | "1" => SCD_1
      case "SCD_2" | "SCD2" | "2" => SCD_2
      case "SCD_3" | "SCD3" | "3" => SCD_3
      case "SCD_UNDEFINED" => SCD_UNDEFINED
      case _ => SCD_UNDEFINED
    }
  }
}