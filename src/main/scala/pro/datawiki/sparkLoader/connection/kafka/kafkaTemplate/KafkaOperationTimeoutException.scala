package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

/** Exception when Kafka operation times out */
class KafkaOperationTimeoutException(message: String) extends RuntimeException(message)
