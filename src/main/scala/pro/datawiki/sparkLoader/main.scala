package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}

@main
def sparkRun(configLocation: String, partition: String, subPartition: String, isDebug: Boolean = false): Unit = {
  val etlConfig = EltConfig.apply(configLocation)
  LogMode.setDebug(isDebug)
  RunConfig.setPartition(partition, subPartition)
  //    try {
  Execute.initConnections(etlConfig.getConnections)
  Execute.setIdmap(etlConfig.getIdmapSource)
  Execute.run(etlConfig)

  etlConfig.getTarget.length match
    case 1 => etlConfig.getTarget.head.writeTarget()
    case 0 => throw Exception()
    case _ => etlConfig.getTarget.foreach(i => {
      try {
        i.writeTarget()
      } catch
        case _ => println("TODO")
    })


  //    } finally {
  //      Connection.closeConnections()
  //    }
}