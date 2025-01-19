package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}
import pro.datawiki.sparkLoader.connection.Connection

@main
def sparkRun(configLocation: String, partition: String): Unit = {
    val etlConfig = EltConfig.apply(configLocation)
    RunConfig.setPartition(partition)
//    try {
      Execute.initConnections(etlConfig.getConnections)
      Execute.setIdmap(etlConfig.getIdmapSource)
      Execute.setCache(etlConfig.getCacheSource)
      Execute.setTarget(etlConfig.getTarget.connection)
      Execute.run(etlConfig)
      Execute.writeTarget(etlConfig.getTarget.targetFile, etlConfig.getTarget.columnsLogicKey)
//    } finally {
//      Connection.closeConnections()
//    }

}