package pro.datawiki.sparkLoader.partition

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pro.datawiki.sparkLoader.configuration.YamlConfigPartitionSync

class PartitionDeltaCheckerSpec extends AnyFlatSpec with Matchers {

  "PartitionDeltaChecker" should "resolve candidate partitions from partitionList" in {
    val config = YamlConfigPartitionSync(
      odsConnection = "test_ods",
      odsTable = "ods.table",
      partitionList = List("2026-06-20", "2026-06-21")
    )
    val candidates = PartitionDeltaChecker.resolveCandidatePartitions(config)
    candidates should contain theSameElementsAs List("2026-06-20", "2026-06-21")
  }

  it should "detect dirty partitions when count differs" in {
    val candidates = List("2026-06-20", "2026-06-21")
    val srcMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1")),
      "2026-06-21" -> PartitionMetrics("2026-06-21", 200L, Some("hash2"))
    )
    val odsMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1")),
      "2026-06-21" -> PartitionMetrics("2026-06-21", 150L, Some("hash2"))
    )

    val dirty = PartitionDeltaChecker.getDirtyPartitions(srcMetrics, odsMetrics, candidates)
    dirty should contain theSameElementsAs List("2026-06-21")
  }

  it should "detect dirty partitions when missing in ODS" in {
    val candidates = List("2026-06-20", "2026-06-21")
    val srcMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1")),
      "2026-06-21" -> PartitionMetrics("2026-06-21", 200L, Some("hash2"))
    )
    val odsMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1"))
    )

    val dirty = PartitionDeltaChecker.getDirtyPartitions(srcMetrics, odsMetrics, candidates)
    dirty should contain theSameElementsAs List("2026-06-21")
  }

  it should "detect dirty partitions when hash differs" in {
    val candidates = List("2026-06-20")
    val srcMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash_new"))
    )
    val odsMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash_old"))
    )

    val dirty = PartitionDeltaChecker.getDirtyPartitions(srcMetrics, odsMetrics, candidates)
    dirty should contain theSameElementsAs List("2026-06-20")
  }

  it should "return empty list when all partitions match" in {
    val candidates = List("2026-06-20", "2026-06-21")
    val srcMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1")),
      "2026-06-21" -> PartitionMetrics("2026-06-21", 200L, Some("hash2"))
    )
    val odsMetrics = Map(
      "2026-06-20" -> PartitionMetrics("2026-06-20", 100L, Some("hash1")),
      "2026-06-21" -> PartitionMetrics("2026-06-21", 200L, Some("hash2"))
    )

    val dirty = PartitionDeltaChecker.getDirtyPartitions(srcMetrics, odsMetrics, candidates)
    dirty shouldBe empty
  }
}
