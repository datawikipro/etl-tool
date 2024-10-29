package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn

case class YamlConfigTarget(connection: String,
                            targetFile:String,
                            autoInsertIdmapCCD: Boolean,
                            columns: List[YamlConfigTargetColumn],
                            columnsLogicKey:List[String],
                            partitionKey:String
                           )
