package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.functions.expr
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.{YamlConfigEltOnServerColdDataFileBasedExtraColumn, YamlConfigEltOnServerColdDataFileBasedFolder}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

class TaskTemplateColdFileBasedTableHugeTable(source: YamlConfigEltOnServerColdDataFileBasedFolder,
                                              target: YamlConfigEltOnServerColdDataFileBasedFolder,
                                              extraColumn: List[YamlConfigEltOnServerColdDataFileBasedExtraColumn],
                                              withBackUp:Boolean,
                                              connection: ConnectionTrait) extends TaskTemplate {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    connection match
      case x: FileStorageTrait => {
        val list = source.getListPartition(x)
        list.foreach(i => {
          var df = x.readDf(source.targetFile, source.partitionBy, List.apply(i), false)
          extraColumn.foreach(i => df = df.withColumn(i.columnName, expr(i.columnTransform)))
          x.writeDfPartitionAuto(df,tableName = target.tableName, target.targetFile, target.partitionBy, WriteMode.autoAppend)
          withBackUp match {
            case true=>x.moveTablePartition(source.targetFile,s"backup2/${target.targetFile}",List.apply(s"${source.partitionBy.head}=$i"))
            case false =>x.deleteFolder(s"${source.targetFile}/${source.partitionBy.head}=$i") 
          }

        })
        return List.empty
      }
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for FileSystem source: ${connection.getClass.getSimpleName}")

  }

}
