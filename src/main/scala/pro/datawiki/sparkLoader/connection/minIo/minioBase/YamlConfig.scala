package pro.datawiki.sparkLoader.connection.minIo.minioBase

case class YamlConfig(minioHost: List[YamlConfigHost],
                      accessKey: String,
                      secretKey: String,
                      bucket: String,
                      // Connection timeout parameters
                      connectionTimeout: Option[String], // ms
                      establishTimeout: Option[String], // ms
                      socketTimeout: Option[String], // ms

                      // File size and multipart upload parameters
                      multipartSize: Option[String], // Size threshold for multipart uploads
                      multipartThreshold: Option[String], // When to use multipart uploads
                      blockSize: Option[String], // Block size for operations

                      // Buffer and performance parameters
                      bufferDir: Option[String], // Buffer directory
                      socketReceiveBuffer: Option[String], // Socket receive buffer size
                      socketSendBuffer: Option[String], // Socket send buffer size

                      // Connection pool parameters
                      maxConnections: Option[String], // Maximum number of connections
                      maxThreads: Option[String], // Maximum number of threads
                      uploadActiveBlocks: Option[String], // Active blocks for upload
                      fastUploadActiveBlocks: Option[String], // Fast upload active blocks

                      // Retry configuration
                      retryLimit: Option[String], // Number of retry attempts
                      retryInterval: Option[String], // Interval between retries
                      attemptsMaximum: Option[String], // Maximum attempts

                      // SSL and security parameters
                      sslEnabled: Option[Boolean], // SSL enabled/disabled
                      pathStyleAccess: Option[Boolean], // Path-style access

                      // Fast upload configuration
                      fastUpload: Option[String], // Enable fast upload//TODO Boolean
                      fastUploadBuffer: Option[String], // Fast upload buffer type

                      // Committer configuration
                      committerName: Option[String], // Committer type
                      committerStagingConflictMode: Option[String], // Conflict resolution mode
                      committerStagingTmpPath: Option[String], // Staging temp path
                      committerStagingUniqueFilenames: Option[String], // Unique filenames//TODO boolean

                      // JSON and file reading parameters
                      jsonMultiline: Option[Boolean], // Enable multiline JSON support
                      jsonMode: Option[String], // JSON parsing mode (PERMISSIVE, DROPMALFORMED, FAILFAST)
                      corruptRecordColumn: Option[String], // Column name for corrupt records

                      // Path and encoding parameters
                      pathEncoding: Option[String], // Path encoding for URL decoding
                      autoDecodeUrls: Option[Boolean], // Automatically decode URL-encoded paths

                      // API timeout and request parameters
                      apiCallTimeout: Option[String], // API call timeout in milliseconds (5 minutes)
                      requestTimeout: Option[String], // Request timeout in milliseconds (5 minutes)
                      clientExecutionTimeout: Option[String], // Client execution timeout (10 minutes)

                      // Temporary file and staging parameters
                      enableCleanupTemporaryFiles: Option[String], // Enable cleanup of temporary files
                      temporaryFileCleanupInterval: Option[String], // Cleanup interval in milliseconds
                      disableTemporaryFiles: Option[String] // Disable temporary file usage
                     )
