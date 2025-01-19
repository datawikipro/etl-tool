package pro.datawiki.sparkLoader.source

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{SegmentationEnum, YamlConfigSource, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCache

object Source {

  def run(source: YamlConfigSource, segmentName: String): Unit = {
    val src = source.getSource
    var df: DataFrame = null

      source.getSegmentation match
      case SegmentationEnum.full => {
        df = src.getDataFrame(sourceName = source.getSourceName)
      }
      case SegmentationEnum.adHoc => {
        val cache = new TransformationCache()
        source.getAdhocRow.foreach(i=> {
          val res =src.getDataFrameAdHoc(sourceName = source.getSourceName, adHoc = i)
          cache.saveTable(res)
        })
        df = cache.readTable
      }
      case _=> throw Exception()

    df.createOrReplaceTempView(source.getObjectName)
  }

  def run(source: List[YamlConfigSource], segmentName: String): Unit = {
    source.foreach(i =>
      run(source = i, segmentName = segmentName))
  }
}
