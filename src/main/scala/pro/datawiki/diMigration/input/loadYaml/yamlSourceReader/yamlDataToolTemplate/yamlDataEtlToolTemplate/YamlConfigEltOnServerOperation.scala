package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateEltOnServerOperation
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskTemplateEltOnServerSQL.CoreTaskTemplateEltOnServerSQL
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL

class YamlConfigEltOnServerOperation(
                                      eltOnServerOperationName: String,
                                      sourceName: String,
                                      sql: YamlConfigEltOnServerSQL,
                                      ignoreError: Boolean) {
  def getCorePreEtlOperations: CoreTaskTemplateEltOnServerOperation = {
    return CoreTaskTemplateEltOnServerOperation(
      eltOnServerOperationName = eltOnServerOperationName,
      sourceName = sourceName,
      sql = sql match {
        case null => null
        case fs => CoreTaskTemplateEltOnServerSQL(
          sql = fs.sql
        )
      },
      ignoreError = ignoreError
    )
  }
}
