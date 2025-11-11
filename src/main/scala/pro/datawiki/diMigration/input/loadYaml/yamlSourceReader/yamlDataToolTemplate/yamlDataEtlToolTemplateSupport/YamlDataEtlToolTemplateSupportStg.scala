package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.YamlDataTemplateSourceFileSystem
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateSource, YamlDataTemplateTarget}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.YamlDataTemplateTargetFileSystem
import pro.datawiki.sparkLoader.dictionaryEnum.{InitModeEnum, WriteMode}

class YamlDataEtlToolTemplateSupportStg( taskName: String,
                                         sourceLogicTableSchema:String,
                                         sourceLogicTableName: String,
                                         yamlFileCoreLocation: String,
                                         yamlFileLocation: String,
                                         sourceCode: String
                                       ) {

  def getStgFolder: String = s"stg/${sourceLogicTableSchema}/${sourceLogicTableName}/run_id=$${run_id}"

  def getStgYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/stg/$taskName.yaml"

  def getStgTaskName: String = s"stg__${taskName}"

  def getStgBatchTaskName: String = s"stg__batch__${taskName}"

  def getStgYamlDataTemplateTarget(inConnection:String): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = null,
      messageBroker = null,
      dummy = null,
      fileSystem = YamlDataTemplateTargetFileSystem(
        connection = inConnection,
        source = "src",
        mode = WriteMode.overwritePartition,
        targetFile = getStgFolder,
        partitionBy = List.apply(),
      ),
      ignoreError = false
    )

  def getOdsYamlDataTemplateSourceYamlDataTemplateSource(inSourceName:String): YamlDataTemplateSource =
    YamlDataTemplateSource(
      sourceName = inSourceName,
      objectName = "src",
      sourceFileSystem = YamlDataTemplateSourceFileSystem(
        tableName = getStgFolder,
        tableColumns = List.apply(),
        partitionBy = List.apply(),
        where = null,
        limit = 0
      ),
      initMode = InitModeEnum.instantly.toString
    )
}
