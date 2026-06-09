package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

/** Exception when Kafka cluster is not healthy */
class KafkaClusterUnhealthyException(message: String) extends RuntimeException(message)
