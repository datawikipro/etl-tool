package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.YamlDataTemplateTarget
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase}
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

class YamlDataEtlToolTemplateSupportOds(taskName: String,
                                        tableSchema: String,
                                        tableName: String,
                                        metadata: TableMetadata,
                                        yamlFileCoreLocation: String,
                                        yamlFileLocation: String,
                                        sourceCode: String) {

  def writeOds(mode:WriteMode,inSource:String): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      fileSystem = null,
      messageBroker = null,
      dummy = null,
      database = YamlDataTemplateTargetDatabase(
        connection = "postgres",
        source = inSource,
        mode = mode,
        partitionMode = null, //TODO
        targetSchema = s"ods__${tableSchema}",
        targetTable = s"${tableName}",
        columns = metadata.columns.map(col =>
          YamlDataTemplateTargetColumn(
            columnName = col.column_name,
            isNullable = true,
            columnType = col.data_type.getTypeInSystem("postgres"),
            columnTypeDecode = col.data_type match {
              case TableMetadataType.TimestampWithoutTimeZone => true
              case _ => false
            }
          )),
        uniqueKey = metadata.primaryKey,
        partitionBy = null,
        scd = "SCD_3"
      ),
      ignoreError = false
    )

}
