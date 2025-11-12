package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceFileSystem, YamlDataTemplateSourceKafka}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.yamlConfigSourceKafka.YamlDataTemplateSourceKafkaTopic
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.YamlDataTemplateTargetFileSystem
import pro.datawiki.sparkLoader.dictionaryEnum.{InitModeEnum, WriteMode}

class YamlDataEtlToolTemplateSupportKafka(
                                           taskName: String,
                                           kafkaTopic:String,
                                           yamlFileCoreLocation: String,
                                           yamlFileLocation: String,
                                           sourceCode: String,
                                           sourceLogicTableSchema:String
                                         ) extends YamlDataEtlToolTemplateSupportBase() {

  def getKafkaTaskName: String = s"kafka__${taskName}"

  def getKafkaYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/kafka/$taskName.yaml"

  def getConnect: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "kafkaUnico",
    connection = "kafkaSaslSSL",
    configLocation = "/opt/etl-tool/configConnection/kafka.yaml"
  )

  def getSource(in:YamlDataTemplateConnect): YamlDataTemplateSource =YamlDataTemplateSource(
    sourceName = in,
    objectName = "source",
    sourceKafka = YamlDataTemplateSourceKafka(
      topics = YamlDataTemplateSourceKafkaTopic(
        topicList = List(kafkaTopic),
      ),
      listTopics = null,
      topicsByRegexp = null
    ),
    initMode = InitModeEnum.instantly.toString
  )

  def getTarget(inConnection:YamlDataTemplateConnect,partitionBy:List[String],inSource:String = "source",
               ): YamlDataTemplateTarget = YamlDataTemplateTarget(
    database = null,
    fileSystem = YamlDataTemplateTargetFileSystem(
      connection = inConnection,
      source = inSource,
      tableName = s"kafka__${sourceLogicTableSchema}.\"${kafkaTopic}\"",
      mode = WriteMode.streamByRunId,
      targetFile = s"kafka/$kafkaTopic",
      partitionBy =partitionBy,
    ),
    messageBroker = null,
    dummy = null,
    ignoreError = false
  )

  def getReadTarget(inSourceName:YamlDataTemplateConnect):YamlDataTemplateSource = YamlDataTemplateSource(
    sourceName = inSourceName,
    objectName = "source",
    sourceFileSystem = YamlDataTemplateSourceFileSystem(
      tableName = s"kafka/$kafkaTopic",
      tableColumns = List.empty,
      partitionBy = List("run_id"),
      where = null,
      limit = 0
    ),
    initMode = InitModeEnum.instantly.toString,
    skipIfEmpty = true
  )

  def getKafkaSource(in:YamlDataTemplateConnect): YamlDataTemplateSource = YamlDataTemplateSource(
    sourceName = in,
    objectName = "source",
    sourceKafka = YamlDataTemplateSourceKafka(
      topics = YamlDataTemplateSourceKafkaTopic(
        topicList = List(kafkaTopic),
      ),
      listTopics = null,
      topicsByRegexp = null
    ),
    initMode = InitModeEnum.instantly.toString
  )
  
  

}
