package pro.datawiki.sparkLoader

import pro.datawiki.sparkLoader.configuration.{EltConfig, RunConfig}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait}
import pro.datawiki.sparkLoader.source.Source
import pro.datawiki.sparkLoader.transformation.{Transformation, TransformationIdMap}

private var target: ConnectionTrait = null

@main
def sparkRun(configLocation: String, partition:String): Unit = {
  val etlConfig = EltConfig.apply(configLocation)
  RunConfig.setPartition(partition)
  try {
    etlConfig.connections.foreach(i => Connection.setConnection(i.sourceName, Connection.initConnection(i.connection, i.configLocation)))
    TransformationIdMap.setIdmap(etlConfig.idmap.sourceName)
    
    Source.run(etlConfig.getSource)
    
    Transformation.run(etlConfig.transformations)

    target = Connection.getConnection(etlConfig.target.connection)

    val df = SparkObject.spark.sql("select * from target")
    target.writeDf(etlConfig.target, df, etlConfig.target.autoInsertIdmapCCD,etlConfig.target.columnsLogicKey)
  } finally {
    Connection.closeConnections()
  }
}