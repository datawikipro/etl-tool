package pro.datawiki.sparkLoader

object SparkRunCLI {

  def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--config" | "-c" => {
          if (i + 1 < args.length) {
            config = config.copy(configLocation = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--config requires a value")
          }
        }
        case "--partition" | "-p" => {
          if (i + 1 < args.length) {
            config = config.copy(partition = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--partition requires a value")
          }
        }
        case "--run_id" | "-p" => {
          if (i + 1 < args.length) {
            config = config.copy(runId = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--run_id requires a value")
          }
        }

        case "--load_date" | "-p" => {
          if (i + 1 < args.length) {
            config = config.copy(loadDate = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--load_date requires a value")
          }
        }
        case "--debug" | "-d" => {
          // Handle both --debug and --debug true/false
          if (i + 1 < args.length && (args(i + 1) == "true" || args(i + 1) == "false")) {
            config = config.copy(isDebug = args(i + 1) == "true")
            i += 1
          } else {
            config = config.copy(isDebug = true)
          }
        }
        case "--help" | "-h" => {
          printUsage()
          System.exit(0)
        }
        case "--version" | "-v" => {
          println("ETL Tool 0.2.0-SNAPSHOT")
          System.exit(0)
        }
        case "--external_progress_loging" => {
          if (i + 1 < args.length) {
            val yamlPath = args(i + 1)
            // Parse YAML file to extract connection and configLocation
            val externalConfig = ExternalProgressLoggingConfig(yamlPath)
            config = config.copy(externalProgressLogging = Some(externalConfig))
            i += 1
          } else {
            throw new IllegalArgumentException("--external_progress_loging requires a YAML file path")
          }
        }
        case "--dag-name" => {
          if (i + 1 < args.length) {
            config = config.copy(dagName = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--dag-name requires a value")
          }
        }
        case "--task-name" => {
          if (i + 1 < args.length) {
            config = config.copy(taskName = args(i + 1))
            i += 1
          } else {
            throw new IllegalArgumentException("--task-name requires a value")
          }
        }
        case arg =>
          throw new IllegalArgumentException(s"Unknown argument: $arg")
      }
      i += 1
    }

    if (config.runId.isEmpty) {
      throw new IllegalArgumentException("--run_id is required")
    }
    if (config.configLocation.isEmpty) {
      throw new IllegalArgumentException("--config is required")
    }
    if (config.partition.isEmpty) {
      throw new IllegalArgumentException("--partition is required")
    }

    config
  }

  def printUsage(): Unit = {
    println(
      """
ETL Tool 0.2.0-SNAPSHOT

Usage: etl-tool [options]

Options:
  -c, --config <path>                    Path to configuration file (required)
  -p, --partition <name>                Partition name (required)
  -d, --debug [true|false]               Enable debug mode (optional value)
  --external_progress_loging <yaml>      Path to external progress logging YAML file (optional)
  --dag-name <name>                     DAG name for ETL progress logging (optional, default: "not defined")
  --task-name <name>                    Task name for ETL progress logging (optional, default: "not defined")
  -h, --help                            Show this help message
  -v, --version                         Show version information

Examples:
  etl-tool --config /path/to/config.yaml --partition partition1
  etl-tool -c config.yaml -p partition1 --debug
  etl-tool --config config.yaml --partition partition1 --debug true
  etl-tool --config config.yaml --partition partition1 --external_progress_loging /path/to/external_logging.yaml
  etl-tool --config config.yaml --partition partition1 --dag-name my_dag --task-name my_task
""")
  }
}