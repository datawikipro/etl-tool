package pro.datawiki.sparkLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pro.datawiki.sparkLoader.traits.LoggingTrait

object SparkObject extends LoggingTrait {
  var localSpark: SparkSession = null

  def initSpark(): Unit = {

    val conf = new SparkConf()
    conf.set("spark.driver.memory", "8g") // Increase driver memory for large Kafka batches
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.executor.heartbeatInterval", "20000")
    conf.set("spark.network.timeout", "600000") // 10 minutes for MongoDB operations
    conf.set("spark.driver.maxResultSize", "4g") // Increase max result size to handle large Kafka batches
    
//     MongoDB-specific configurations for better cursor handling
    conf.set("spark.mongodb.read.cursorTimeoutMS", "0") // Disable cursor timeout globally
    conf.set("spark.mongodb.read.noCursorTimeout", "true") // Prevent cursor timeout globally
    conf.set("spark.mongodb.read.maxTimeMS", "600000") // 10 minutes max operation time
    conf.set("spark.mongodb.read.maxAwaitTimeMS", "30000") // 30 seconds max await time
    conf.set("spark.mongodb.read.batchSize", "1000") // Optimize batch size
    conf.set("spark.mongodb.read.maxBatchSize", "1000") // Optimize max batch size
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.driver.extraJavaOptions", "java.base/sun.nio.cs=ALL-UNNAMED")
    conf.set("spark.executor.extraJavaOptions", "java.base/sun.nio.cs=ALL-UNNAMED")

//     Additional configuration for better file writing performance (optimized for large datasets)
    conf.set("spark.sql.files.maxPartitionBytes", "268435456") // 256MB per partition (increased for large files)
    conf.set("spark.sql.files.openCostInBytes", "8388608") // 8MB (increased)
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") // 128MB (increased)
//     Using default Java serializer instead of Kryo to avoid Java 17 module issues
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

//     MinIO/S3A specific timeout configurations (fallback defaults - actual values come from MinIO config)
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "1800000") // 30 minutes connection timeout
    conf.set("spark.hadoop.fs.s3a.api.call.timeout", "1800000") // 30 minutes API call timeout
    conf.set("spark.hadoop.fs.s3a.request.timeout", "1800000") // 30 minutes request timeout
    conf.set("spark.hadoop.fs.s3a.client.execution.timeout", "3600000") // 60 minutes client execution timeout

//     Additional reliability configurations (increased for large datasets)
    conf.set("spark.hadoop.fs.s3a.retry.limit", "20") // Increase retry attempts
    conf.set("spark.hadoop.fs.s3a.retry.interval", "5000ms") // Increase retry interval
    conf.set("spark.hadoop.fs.s3a.attempts.maximum", "10") // Increase maximum attempts

//     AWS SDK specific configurations to override default 60s timeout (increased for large datasets)
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") // 1 minute
    conf.set("spark.hadoop.fs.s3a.socket.timeout", "300000") // 5 minutes
    conf.set("spark.hadoop.fs.s3a.retry.policy.attempts", "20") // Increased for large datasets
    conf.set("spark.hadoop.fs.s3a.retry.policy.sleep.initial", "2000ms") // Increased initial sleep
    conf.set("spark.hadoop.fs.s3a.retry.policy.sleep.max", "30000ms") // Increased max sleep

    // Temporary file and staging optimizations
    conf.set("spark.sql.streaming.checkpointLocation.deleteTmpCheckpointDir", "true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

    // Prevent premature SparkContext shutdown during long-running operations
    conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") // Reduce batch size for stability
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456") // 256MB
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    println("------------------------Start session------------------------------------")
    localSpark = SparkSession.builder().
      appName("etl-tool").
      config(conf).
//      master("local[*]").
      master("spark://localhost:7077").
      getOrCreate()
    localSpark.sparkContext.setLogLevel("ERROR")

    println("------------------------Start end session--------------------------------")
  }

  def spark: SparkSession = {
    if localSpark == null then initSpark()

    return localSpark
  }

  def setHadoopConfiguration(key: String, value: String): Unit = {
    spark.sparkContext.hadoopConfiguration.set(key, value)
  }

}