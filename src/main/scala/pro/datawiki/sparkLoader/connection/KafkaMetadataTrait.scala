package pro.datawiki.sparkLoader.connection

trait KafkaMetadataTrait extends MetadataTrait {
  def readKafkaOffsets(metadataSchema: String, topic: String): Option[String]
  def updateKafkaOffsets(metadataSchema: String, modify: Map[(String, Int), Long] ): Unit
}
