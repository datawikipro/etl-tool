package pro.datawiki.sparkLoader.partition

case class PartitionMetrics(
                             partitionValue: String,
                             count: Long,
                             hashValue: Option[String] = None
                           )
