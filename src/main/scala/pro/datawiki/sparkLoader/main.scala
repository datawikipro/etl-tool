package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}

@main
def sparkRun(configLocation: String, partition: String, subPartition: String, isDebug: Boolean = false): Unit = {
  val etlConfig = EltConfig.apply(configLocation)
  LogMode.setDebug(isDebug)
  RunConfig.setPartition(partition,subPartition)
  //    try {
  Execute.initConnections(etlConfig.getConnections)
  Execute.setIdmap(etlConfig.getIdmapSource)
  Execute.setCache(etlConfig.getCacheSource)
  Execute.setTarget(etlConfig.getTarget.connection)
  Execute.run(etlConfig)
  Execute.writeTarget(etlConfig.getTarget)
  //    } finally {
  //      Connection.closeConnections()
  //    }
}