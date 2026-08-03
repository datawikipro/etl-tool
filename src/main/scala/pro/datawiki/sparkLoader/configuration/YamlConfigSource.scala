package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonAlias, JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.InitModeEnum
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateReadEmail}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.LogicClass

import java.time.LocalDateTime
import java.util.TimeZone

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSource(
                             sourceName: String,
                             objectName: String,
                             segmentation: String,
                             @JsonAlias(Array("jdbc")) sourceDb: YamlConfigSourceDBTable,
                             sourceSQL: YamlConfigSourceDBSQL,
                             sourceFileSystem: YamlConfigSourceFileSystem,
                             sourceKafka: YamlConfigSourceKafka,
                             sourceWeb: YamlConfigSourceWeb,
                             sourceMail: YamlConfigSourceMail,
                             sourceBigQuery: YamlConfigSourceBigQuery,
                             initMode: String,
                             skipIfEmpty: Boolean = false,
                             partitionSync: Option[YamlConfigPartitionSync] = None
                           ) extends LoggingTrait {
  @JsonIgnore
  private def initModeEnum: InitModeEnum = InitModeEnum(initMode)

  @JsonIgnore
  def createTask(): Task = {
    
    val logic = LogicClass.getLogic(sourceDb, sourceSQL, sourceFileSystem, sourceKafka, sourceWeb, sourceMail, sourceBigQuery)
    
    val taskTemplate: TaskTemplate = logic match
      case x: YamlConfigSourceTrait => x.getTaskTemplate(ApplicationContext.getConnection(sourceName))
      case other => throw ConfigurationException(s"Неизвестный тип источника: '$other'. Пожалуйста, проверьте конфигурацию.")

    partitionSync match {
      case Some(sync) if sync.enabled =>
        val candidates = pro.datawiki.sparkLoader.partition.PartitionDeltaChecker.resolveCandidatePartitions(sync)
        if (candidates.nonEmpty) {
          logInfo(s"PartitionDeltaChecker: Checking ${candidates.length} candidate partitions for source object: $objectName")
          val srcConn = ApplicationContext.getConnection(sourceName)
          val odsConn = try {
            ApplicationContext.getConnection(sync.odsConnection)
          } catch {
            case _: Exception =>
              sync.odsConfigLocation match {
                case Some(loc) => pro.datawiki.sparkLoader.connection.ConnectionTrait(sync.odsConnection, sync.odsConnection, loc)
                case None => srcConn
              }
          }

          val srcTable = if (sourceDb != null) s"${sourceDb.tableSchema}.${sourceDb.tableName}"
          else if (sourceFileSystem != null) sourceFileSystem.tableName
          else objectName

          val srcWhere = if (sourceDb != null) sourceDb.filter
          else if (sourceFileSystem != null) sourceFileSystem.where
          else null

          val srcMetrics = pro.datawiki.sparkLoader.partition.PartitionDeltaChecker.fetchMetricsFromSource(srcConn, srcTable, srcWhere, sync, candidates)
          val odsMetrics = pro.datawiki.sparkLoader.partition.PartitionDeltaChecker.fetchMetricsFromOds(odsConn, sync, candidates)

          val dirtyPartitions = pro.datawiki.sparkLoader.partition.PartitionDeltaChecker.getDirtyPartitions(srcMetrics, odsMetrics, candidates)

          if (dirtyPartitions.isEmpty) {
            logInfo(s"PartitionDeltaChecker: All ${candidates.length} candidate partitions match ODS for $objectName. Skipping source task.")
            return new Task {
              override def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = ProgressStatus.skip
            }
          } else {
            logInfo(s"PartitionDeltaChecker: Found ${dirtyPartitions.length} dirty partitions for $objectName: ${dirtyPartitions.mkString(", ")}")
            ApplicationContext.setGlobalVariable("dirty_partitions", dirtyPartitions.map(p => s"'$p'").mkString(","))
          }
        }
      case _ =>
    }

    val task: Task = initModeEnum match
      case InitModeEnum.instantly => TaskSimple(taskTemplate,skipIfEmpty)
      case InitModeEnum.adHoc => TaskAdHocRegister(taskTemplate)
      case InitModeEnum.consumer => {
        val timeZone: TimeZone = TimeZone.getTimeZone("UTC")
        taskTemplate match {
          case x: TaskTemplateReadEmail => {
            val currentDateTime: LocalDateTime = LocalDateTime.now()
            x.setTime(currentDateTime)

            TaskConsumer(x,skipIfEmpty)
          }
          case other => throw ConfigurationException(s"Неподдерживаемый тип источника данных: '$other'. Проверьте конфигурацию источника.")
        }

      }
      case InitModeEnum.runAtServer => TaskRunAtServerRegister(taskTemplate)
      case null => {
        throw UnsupportedOperationException("Unsupported configuration source case")
      }
    return task
  }

}