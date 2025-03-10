package pro.datawiki.sparkLoader.source

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{SegmentationEnum, YamlConfigSource, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCache

object Source {

  var isEmpty = true

  def run(source: YamlConfigSource, segmentName: String): Unit = {
    val src = source.getSource
    var df: DataFrame = null

    source.getSegmentation match
      case SegmentationEnum.full => {
        df = src.getDataFrame(sourceName = source.getSourceName)
      }
      case SegmentationEnum.adHoc => {
        val cache: TransformationCache = TransformationCache.apply()
        val rows = source.getAdhocRow
        if rows.isEmpty then throw Exception()
        rows.foreach(i => {
          try {
            val tmp = src.getDataFrameAdHoc(sourceName = source.getSourceName, adHoc = i)
            val res: DataFrame = tmp._1
            val partition: String = tmp._2

            cache.saveTable(res)
          } catch
            case _ => println("Skip")

        })
        df = cache.readTable
      }
      case _ => throw Exception()

    df.createOrReplaceTempView(source.getObjectName)
  }

  def run(source: List[YamlConfigSource], segmentName: String): Unit = {
    source.foreach(i =>
      run(source = i, segmentName = segmentName))
  }
}
