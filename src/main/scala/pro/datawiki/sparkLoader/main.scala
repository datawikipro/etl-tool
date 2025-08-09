package pro.datawiki.sparkLoader

import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}
import pro.datawiki.sparkLoader.task.Context

@main
def sparkRun(configLocation: String, partition: String, subPartition: String, isDebug: Boolean = false): Unit = {
  try {
    LogMode.setDebug(isDebug)
    ////////--------------------------------------------
    //  val df =SchemaValidator.getDataFrameFromJson(
    //    Files.readString(Paths.get("hall.json")),
    //    "/opt/etl-tool/config/schemas/jsonApiHallPlanValid.yaml"
    //  )
    //  LogMode.debugDF(df)
    ////--------------------------------------------
    //  throw Exception()

    // Init Config 
    RunConfig.initPartitions(partition, subPartition)
    if false then {
      while (true) {
        EltConfig(configLocation) match {
          case done =>
          case skip =>
          case error => throw new DataProcessingException("ETL process failed with error status")
          case _ => throw new DataProcessingException("ETL process returned unexpected status")
        }
        println("next")
      }
    } else {
      val result = EltConfig(configLocation) match {
        case done =>
        case skip => 
        case error => throw new DataProcessingException("ETL process failed with error status")
        case _ => throw new DataProcessingException("ETL process returned unexpected status")
      }
    }
    Context.closeConnections()
  } catch {
    case e: Exception =>
      println(s"ETL process failed: ${e.getMessage}")
      e.printStackTrace()
      throw e
  }
}