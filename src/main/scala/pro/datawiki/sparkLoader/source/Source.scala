package pro.datawiki.sparkLoader.source

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.{SegmentationEnum, YamlConfigSource, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheFileStorage, TransformationCacheTrait}

object Source {

  var isEmpty = true

  def run(source: YamlConfigSource, segmentName: String): Boolean = {
    val src = source.getSource

    source.getSegmentation match
      case SegmentationEnum.full => {
        val df = src.getDataFrame(sourceName = source.getSourceName)
        df.get.createOrReplaceTempView(source.getObjectName)
        return true
      }
      case SegmentationEnum.adHoc => {
        var df: List[DataFrameTrait] = null
        val cache: TransformationCacheTrait = source.getCache
        val rows = source.getAdhocRow
        if rows.isEmpty then throw Exception()
        var skipped: Int = 0
        rows.foreach(i => {
//          try {
            val res: DataFrameTrait =  src.getDataFrameAdHoc(sourceName = source.getSourceName, adHoc = i)
            cache.saveTable(res)
//          } catch
//            case _ => skipped = skipped+1
        })
        if skipped > 0 then println(s"skipped ${skipped}")
        df = cache.readDirty

        df.length match
          case 1 => df.head.getPartitionName match
            case "" => df.head.get.createOrReplaceTempView(s"${source.getObjectName}")
            case _ => df.head.get.createOrReplaceTempView(s"${source.getObjectName}__${df.head.getPartitionName}"  )
          case _ => {
            df.foreach(i => {
              i.get.createOrReplaceTempView(s"${source.getObjectName}__${i.getPartitionName}")
            })
          }
        return true
      }
      case _ => throw Exception()

  }

  def run(source: List[YamlConfigSource], segmentName: String): Unit = {
    source.foreach(i =>
      run(source = i, segmentName = segmentName))
  }
}
