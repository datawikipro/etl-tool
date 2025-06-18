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

  val etlConfig = EltConfig(configLocation)

  Context.closeConnections()

}