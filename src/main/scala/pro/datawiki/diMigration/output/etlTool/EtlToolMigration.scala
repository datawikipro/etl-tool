package pro.datawiki.diMigration.output.etlTool

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.task.CoreTaskEtlToolTemplate
import pro.datawiki.diMigration.output.traits.TargetMigration
import pro.datawiki.sparkLoader.configuration.*
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetColumn, YamlConfigTargetDatabase, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.*
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.{YamlConfigTransformationIdMapBaseConfig, YamlConfigTransformationIdMapConfig, YamlConfigTransformationIdMapMerge}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

class EtlToolMigration(targetLocation: String, templateLocation: String) extends TargetMigration {

  override def exportDag(in: CoreDag): Unit = {
    in match {
      case x: CoreBaseDag => {
        var row: mutable.Map[String, String] = mutable.Map.apply()
        row += ("dagName" -> x.dagName)
        row += ("dagDescription" -> x.dagDescription)
        row += ("schedule" -> x.schedule.toString)
        row += ("tags" -> x.tags.map(col => s"'${col}'").mkString(","))
        var text = YamlClass.getLines(s"$templateLocation/dagTemplate.py", row)

        x.listTask.foreach {
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
                        partitionBy = fs.partitionBy,
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
                      listTopics = YamlConfigSourceKafkaListTopics(template = fs.listTopics.template),
                      topicsByRegexp = YamlConfigSourceKafkaTopicsByRegexp(template = fs.topicsByRegexp.template))
                  },
                  sourceWeb = con.sourceWeb match {
                    case null => null
                    case fs =>
                      YamlConfigSourceWeb(
                        run = fs.run,
                        isDirty = fs.isDirty)
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
                  cache = con.cache,
                  initMode = con.initMode,
                  skipIfEmpty = con.skipIfEmpty
                )
              ),
              transformations = x.transform.map(con => {
                YamlConfigTransformation(
                  objectName = con.objectName,
                  cache = con.cache,
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
                      YamlConfigTransformationSparkSql(
                        sql = fs.sql,
                        isLazyTransform = fs.isLazyTransform,
                        lazyTable = fs.lazyTable)
                  },
                  extractSchema = con.extractSchema match {
                    case null => null
                    case fs =>
                      YamlConfigTransformationExtractSchema(
                        tableName = fs.tableName,
                        jsonColumn = fs.jsonColumn,
                        jsonResultColumn = fs.jsonResultColumn,
                        baseSchema = fs.baseSchema)
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
                        partitionMode = fs.partitionMode,
                        targetTable = fs.targetTable,
                        columns = fs.columns.map(col =>
                          YamlConfigTargetColumn(
                            columnName = col.columnName,
                            isNewCCD = col.isNewCCD,
                            domainName = col.domainName,
                            tenantName = col.tenantName,
                            isNullable = col.isNullable
                          )
                        ),
                        uniqueKey = fs.uniqueKey,
                        deduplicationKey = fs.deduplicationKey,
                        partitionBy = fs.partitionBy)
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
                  ignoreError = con.ignoreError
                )
              )
            )

            val yamlText = YamlClass.toYaml(exp)
            YamlClass.writefile(s"${x.yamlFile}", yamlText)

          }
        }
        YamlClass.writefile(s"${x.pythonFile}", text)
      }
      case _ => {
        throw Exception()
      }
    }
  }
}
