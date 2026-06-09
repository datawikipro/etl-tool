package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

/** Cluster health information */
case class ClusterHealthInfo(
  clusterId: String,
  nodeCount: Int,
  controllerId: Option[Int],
  controllerHost: Option[String]
)
