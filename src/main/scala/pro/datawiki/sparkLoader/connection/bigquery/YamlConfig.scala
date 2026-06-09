package pro.datawiki.sparkLoader.connection.bigquery

case class YamlConfig(
                       projectId: String,
                       credentialsJson: String = null,
                       //                        location: String = "US",
                       //                        applicationName: String = "etl-tool",
                       //                        // Настройки производительности
                       //                        maxParallelism: Int = 4,
                       //                        enableBatchSize: Boolean = true,
                       //                        batchSize: Int = 1000,
                       //                        // Настройки таймаутов
                       //                        readTimeout: Int = 300, // секунды
                       //                        writeTimeout: Int = 600, // секунды
                       //                        // Дополнительные параметры BigQuery
                       //                        useQueryCache: Boolean = true,
                       //                        useLegacySql: Boolean = false,
                       //                        enableStandardSql: Boolean = true,
                       //                        materializationProject: String = null, // для промежуточных таблиц
                       //                        materializationDataset: String = null, // для промежуточных таблиц
                       //                        // Настройки Google Cloud Storage (для временных файлов)
                       //                        temporaryGcsBucket: String = null,
                       //                        // Настройки кэширования и оптимизации
                       //                        enableListInference: Boolean = true,
                       //                        enableModeCheckForSchemaFields: Boolean = true,
                       //                        // Настройки сжатия
                       //                        compressionType: String = "GZIP" // NONE, GZIP, DEFLATE, SNAPPY, LZ4
                     )
