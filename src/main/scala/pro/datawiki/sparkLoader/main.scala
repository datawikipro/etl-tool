package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}
import pro.datawiki.sparkLoader.task.Context

@main
def sparkRun(configLocation: String, partition: String, subPartition: String, isDebug: Boolean = false): Unit = {
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
//  while (true) {
//    val etlConfig = EltConfig(configLocation)
//    println("next")
//  }
  EltConfig(configLocation) match {
    case done =>
    case skip =>
    case error=> throw Exception()
    case _=> throw Exception()
  }

  Context.closeConnections()

}