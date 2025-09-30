package pro.datawiki.diMigration.core.dictionary

import pro.datawiki.exception.NotImplementedException

enum Schedule {
  case All, Daily
  , Hourly
  , None
  , Realtime
  ,DailyAtThree,
  EveryTenMinutes

  def getAirflowFormat: String = {
    this match
      case `All` => return """"* * * * *""""
      case `Daily` => return throw NotImplementedException("Daily schedule format not implemented")
      case `Hourly` => return """"0 * * * *""""
      case `None` => return throw NotImplementedException("None schedule format not implemented")
      case `Realtime` => return """"* * * * *""""
      case `DailyAtThree` => """"0 3 * * *""""
      case `EveryTenMinutes` => """"*/10 * * * *""""
  }
}

object Schedule {
  def apply(schedule: String): Schedule = {
    schedule match
      case "hourly" => Schedule.Hourly
      case "daily" => Schedule.Daily
      case "all" => Schedule.All
      case "realtime" => Schedule.Realtime
      case "DailyAtThree" =>Schedule.DailyAtThree
      case "EveryTenMinutes"=> Schedule.EveryTenMinutes
      case fs => {
        throw Exception()
      }
  }

}