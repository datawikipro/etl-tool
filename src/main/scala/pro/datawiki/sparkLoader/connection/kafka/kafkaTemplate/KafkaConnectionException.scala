package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

/** Exception when cannot connect to any Kafka broker */
class KafkaConnectionException(message: String) extends RuntimeException(message)
