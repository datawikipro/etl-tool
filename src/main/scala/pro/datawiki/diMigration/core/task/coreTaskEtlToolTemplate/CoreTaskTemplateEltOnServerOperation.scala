package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskTemplateEltOnServerSQL.CoreTaskTemplateEltOnServerSQL
import pro.datawiki.sparkLoader.configuration.YamlConfigEltOnServerOperation
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL

case class CoreTaskTemplateEltOnServerOperation(
                                                 eltOnServerOperationName: String,
                                                 sourceName: String,
                                                 sql: CoreTaskTemplateEltOnServerSQL,
                                                 ignoreError: Boolean
                                               ) {

  def getEtToolFormat: YamlConfigEltOnServerOperation = YamlConfigEltOnServerOperation(
    eltOnServerOperationName = eltOnServerOperationName,
    sourceName = sourceName,
    sql = sql match {
      case null => null
      case fs => YamlConfigEltOnServerSQL(sql = fs.sql)
    },
    coldDataFileBased = null, //TODO


    //                        coldDataFileBased match {
    //                          case null => null
    //                          case fs => YamlConfigEltOnServerColdDataFileBased(
    //                            source = throw Exception(),
    //                            target = throw Exception(),
    //                            method = fs.method
    //                          )
    //                        },

    ignoreError = ignoreError
  )
}
