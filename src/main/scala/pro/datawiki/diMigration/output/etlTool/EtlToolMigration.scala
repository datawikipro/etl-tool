package pro.datawiki.diMigration.output.etlTool

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.task.CoreTaskEtlToolTemplate
import pro.datawiki.diMigration.output.traits.TargetMigration
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb.YamlConfigSchema
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetDatabase.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetDatabase, YamlConfigTargetDummy, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.{YamlConfigTransformationIdMapBaseConfig, YamlConfigTransformationIdMapConfig, YamlConfigTransformationIdMapMerge}
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.*
import pro.datawiki.sparkLoader.configuration.*
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressMode
import pro.datawiki.yamlConfiguration.YamlClass

import java.text.SimpleDateFormat
import scala.collection.mutable

class EtlToolMigration(targetLocation: String, templateLocation: String) extends TargetMigration {

  override def exportDag(in: CoreDag): Unit = {
    in match {
      case x: CoreBaseDag => {
        var row: mutable.Map[String, String] = mutable.Map.apply()
        row += ("dagName" -> x.dagName)
        row += ("dagDescription" -> x.dagDescription)
        row += ("catchup" ->  (x.catchup match {
          case true => "True"
          case false => "False"
        }))
        row += ("start_date" -> (x.startDate match {
          case null => "days_ago(0)"
          case date => {
            val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
            s"datetime(${date.getYear + 1900}, ${date.getMonth + 1}, ${date.getDate})"
          }
        }))
        row += ("schedule" -> x.schedule.getAirflowFormat)
        row += ("tags" -> x.tags.map(col => s"'${col}'").mkString(","))
        row += ("defaultPipe" -> x.taskPipelines.head.head.getTaskId)
        var text = YamlClass.getLines(s"$templateLocation/dagTemplate.py", row)

        x.taskPipelines.foreach(x => {
          x.foreach {
            case x: CoreTaskEtlToolTemplate => {
              var row2: mutable.Map[String, String] = row
              row2 += ("taskName" -> x.taskName)
              row2 += ("yamlFile" -> x.yamlFile)
              x.dependencies.length match {
                case 0 => row2 += ("dependencies" -> "")
                case _ => row2 += ("dependencies" -> s"[${x.dependencies.mkString(", ")}] >> ${x.taskName}")
              }

              text += s"\n${YamlClass.getLines(s"$templateLocation/taskTemplate.py", row2)}"

              val exp: EltConfig = new EltConfig(
                connections = x.connections.map(con =>
                  YamlConfigConnections(
                    sourceName = con.sourceName,
                    connection = con.connection,
                    configLocation = con.configLocation)
                ),
                preEtlOperations = x.preEtlOperations.map(con => {
                  YamlConfigEltOnServerOperation(
                    eltOnServerOperationName = con.eltOnServerOperationName,
                    sourceName = con.sourceName,
                    sql = con.sql match {
                      case null => null
                      case fs => YamlConfigEltOnServerSQL(sql = fs.sql)
                    },
                    ignoreError = con.ignoreError
                  )
                }),
                source = x.sources.map(con =>
                  YamlConfigSource(
                    sourceName = con.sourceName,
                    objectName = con.objectName,
                    segmentation = con.segmentation,
                    sourceDb = con.sourceDb match {
                      case null => null
                      case fs =>
                        YamlConfigSourceDBTable(
                          tableSchema = fs.tableSchema,
                          tableName = fs.tableName,
                          tableColumns = fs.tableColumns,
                          filter = fs.filter,
                          limit = fs.limit
                        )
                    },
                    sourceSQL = con.sourceSQL match {
                      case null => null
                      case fs =>
                        YamlConfigSourceDBSQL(
                          sql = con.sourceSQL.sql
                        )
                    },
                    sourceFileSystem = con.sourceFileSystem match {
                      case null => null
                      case fs =>
                        YamlConfigSourceFileSystem(
                          tableName = fs.tableName,
                          tableColumns = fs.tableColumns.map(col =>
                            YamlConfigSourceDBTableColumn(
                              columnName = col.columnName,
                              columnType = col.columnType
                            )
                          ),
                          partitionBy = fs.partitionBy,
                          where = fs.where,
                          limit = fs.limit)
                    },
                    sourceKafka = con.sourceKafka match {
                      case null => null
                      case fs => YamlConfigSourceKafka(
                        topics = YamlConfigSourceKafkaTopic(topicList = fs.topics.topicList),
                        listTopics = fs.listTopics match {
                          case null => null
                          case fs => YamlConfigSourceKafkaListTopics(template = fs.template)
                        },
                        topicsByRegexp = fs.topicsByRegexp match {
                          case null => null
                          case fs => YamlConfigSourceKafkaTopicsByRegexp(template = fs.template)
                        }
                      )
                    },
                    sourceWeb = con.sourceWeb match {
                      case null => null
                      case fs =>
                        YamlConfigSourceWeb(
                          run = fs.run,
                          isDirty = fs.isDirty,
                          schemas = fs.schemas.map(col =>YamlConfigSchema(
                            schemaName = col.schemaName,
                            fileLocation = col.fileLocation,
                            isError = col.isError
                          )   )
                        )
                    },
                    sourceMail = con.sourceMail match {
                      case null => null
                      case fs =>
                        YamlConfigSourceMail(
                          email = fs.email,
                          password = fs.password,
                          from = fs.from,
                          subject = fs.subject)
                    },
                    sourceBigQuery = con.bigQuery match {
                      case null => null
                      case fs =>
                        YamlConfigSourceBigQuery(
                          projectId = fs.projectId,
                          datasetId = fs.datasetId,
                          tableId = fs.tableId
                        )
                    },
                    initMode = con.initMode,
                    skipIfEmpty = con.skipIfEmpty
                  )
                ),
                transformations = x.transform.map(con => {
                  YamlConfigTransformation(
                    objectName = con.objectName,
                    idMap = con.idMap match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationIdMap(
                          sourceName = fs.sourceName,
                          connection = fs.connection,
                          idMapGenerate = fs.idMapGenerate.map(col =>
                            YamlConfigTransformationIdMapConfig(
                              systemCode = col.systemCode,
                              columnNames = col.columnNames,
                              domainName = col.domainName,
                              alias = col.alias
                            )
                          ),
                          idMapRestore = fs.idMapRestore.map(col =>
                            YamlConfigTransformationIdMapConfig(
                              systemCode = col.systemCode,
                              columnNames = col.columnNames,
                              domainName = col.domainName,
                              alias = col.alias
                            )
                          ),
                          idMapMerge = fs.idMapMerge.map(col =>
                            YamlConfigTransformationIdMapMerge(
                              alias = col.alias,
                              restoreRk = col.restoreRk,
                              in =
                                YamlConfigTransformationIdMapBaseConfig(
                                  systemCode = col.in.systemCode,
                                  columnNames = col.in.columnNames,
                                  domainName = col.in.domainName,
                                ),
                              out =
                                YamlConfigTransformationIdMapBaseConfig(
                                  systemCode = col.out.systemCode,
                                  columnNames = col.out.columnNames,
                                  domainName = col.out.domainName,
                                )
                            )
                          ))
                    },
                    sparkSql = con.sparkSql match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationSparkSql(                      sql = fs.sql)
                    },
                    sparkSqlLazy = con.sparkSqlLazy match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationSparkSqlLazy(
                          sql = fs.sql,
                          lazyTable = fs.lazyTable)
                    },

                    extractSchema = con.extractSchema match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationExtractSchema(
                          tableName = fs.tableName,
                          jsonColumn = fs.jsonColumn,
                          jsonResultColumn = fs.jsonResultColumn,
                          baseSchema = fs.baseSchema,
                          mergeSchema =  fs.mergeSchema,
                          loadMode = ProgressMode.batch.toString2
                        )
                    },
                    extractAndValidateDataFrame = con.extractAndValidateDataFrame match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationExtractAndValidateDataFrame(
                          dataFrameIn = fs.dataFrameIn,
                          configLocation = fs.configLocation,
                        )
                    },
                    adHoc = con.adHoc match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationAdHoc(
                          sourceObjectName = fs.sourceObjectName,
                          templateName = fs.templateName,
                          columnId = fs.columnId,
                          asyncNumber = fs.asyncNumber
                        )
                    },
                    deduplicate = con.deduplicate match {
                      case null => null
                      case fs =>
                        YamlConfigTransformationDeduplicate(
                          sourceTable = fs.sourceTable,
                          uniqueKey = fs.uniqueKey,
                          deduplicationKey = fs.deduplicationKey
                        )
                    }
                  )
                }
                ),
                target = x.target.map(con =>
                  YamlConfigTarget(
                    database = con.database match {
                      case null => null
                      case fs =>
                        YamlConfigTargetDatabase(
                          connection = fs.connection,
                          source = fs.source,
                          mode = fs.mode.getExportString,
                          targetSchema = fs.targetSchema,
                          targetTable = fs.targetTable,
                          columns = fs.columns.map(col =>
                            YamlConfigTargetColumn(
                              columnName = col.columnName,
                              isNullable = col.isNullable,
                              columnType = col.columnType,
                              columnTypeDecode = col.columnTypeDecode
                            )
                          ),
                          uniqueKey = fs.uniqueKey,
                          partitionBy = fs.partitionBy,
                            scd = fs.scd)
                    },
                    fileSystem = con.fileSystem match {
                      case null => null
                      case fs =>
                        YamlConfigTargetFileSystem(
                          connection = fs.connection,
                          source = fs.source,
                          mode = fs.mode.getExportString,
                          partitionMode = fs.partitionMode,
                          targetFile = fs.targetFile,
                          partitionBy = fs.partitionBy
                        )
                    },
                    messageBroker = con.messageBroker match {
                      case null => null
                      case fs =>
                        YamlConfigTargetMessageBroker(
                          connection = fs.connection,
                          source = fs.source,
                          mode = fs.mode,
                          target = fs.target,
                          partitionMode = fs.partitionMode
                        )
                    },
                    dummy = con.dummy match {
                      case null => null
                      case fs => YamlConfigTargetDummy()
                    },
                    ignoreError = con.ignoreError
                  )
                )
              )

              val yamlText = YamlClass.toYaml(exp)
              YamlClass.writefile(s"${x.yamlFile}", yamlText)

            }
          }
          text += s"\n    branching >> ${x.head.getTaskId}"
        })
        YamlClass.writefile(s"${x.pythonFile}", text)

      }
      case _ => {
        throw UnsupportedOperationException("Unsupported migration case")
      }
    }
  }
}
