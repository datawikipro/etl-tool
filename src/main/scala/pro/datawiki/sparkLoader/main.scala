package pro.datawiki.sparkLoader

import pro.datawiki.schemaValidator.SchemaValidator
import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}
import pro.datawiki.sparkLoader.connection.Connection

import java.nio.file.{Files, Paths}

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
  val etlConfig = EltConfig.apply(configLocation)

  RunConfig.setPartition(partition, subPartition)
  Execute.initConnections(etlConfig.getConnections)
  Execute.setIdmap(etlConfig.getIdmapSource)
  Execute.run(etlConfig)

  etlConfig.getTarget.length match
    case 1 => etlConfig.getTarget.head.writeTarget()
    case 0 => throw Exception()
    case _ => etlConfig.getTarget.foreach(i => {
      i.writeTarget()
    })

  Connection.closeConnections()

}