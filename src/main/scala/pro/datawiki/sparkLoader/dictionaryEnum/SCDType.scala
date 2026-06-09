package pro.datawiki.sparkLoader.dictionaryEnum

enum SCDType {
  case SCD_0, SCD_1, SCD_2, SCD_3, SCD_UNDEFINED
}

object SCDType {
  def apply(in: String): SCDType = {
    in match {
      case "SCD_0" => return SCD_0
      case "SCD_1" => return SCD_1
      case "SCD_2" => return SCD_2
      case "SCD_3" => return SCD_3
      case "SCD_UNDEFINED" => SCD_UNDEFINED
      case _ => throw Exception()
    }
  }
}