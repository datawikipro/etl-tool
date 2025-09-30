package pro.datawiki.sparkLoader.dictionaryEnum

enum InitModeEnum {
  case instantly, adHoc, consumer
}

object InitModeEnum {
  def apply(in: String): InitModeEnum = {
    in match
      case "instantly" => InitModeEnum.instantly
      case "adHoc" => InitModeEnum.adHoc
      case "consumer" => InitModeEnum.consumer
      case _ => throw Exception("initMode not defined")
  }
}