import sbt.Keys.libraryDependencies

import scala.collection.Seq

ThisBuild / version := "0.2.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "etl-tool",
    assembly / assemblyJarName := "etl-tool.jar",
    assembly / mainClass := Some("pro.datawiki.sparkLoader.sparkRun"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs map {_.toLowerCase} match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
            MergeStrategy.discard
          case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case "services" :: _ =>  MergeStrategy.filterDistinctLines
          case _ => MergeStrategy.first
        }
      case _ => MergeStrategy.first
    }
  )

val sparkVersion = "3.5.6"
val hadoopVersion = "3.4.1"
val jacksonDataformatVersion = "2.18.2"
val json4sVersion = "3.7.0-M11"
val awsSdkVersion = "1.12.783"
val postgresqlVersion = "42.7.5"
val clickhouseVersion = "0.8.2"
val mailVersion = "1.6.2"
val mysqlVersion = "8.0.33"
val jodaTimeVersion = "2.13.0"
val ioMinioVersion =  "8.5.17"
val googleApiVersion = "33.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDataformatVersion,
).map(_.cross(CrossVersion.for3Use2_13))

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "com.github.scopt" %% "scopt" % "4.1.0",
  "joda-time" % "joda-time" % jodaTimeVersion,

  "com.amazonaws" % "aws-java-sdk" % awsSdkVersion,
  "com.amazonaws" % "aws-java-sdk-s3" % awsSdkVersion,
  "com.amazonaws" % "aws-java-sdk-bundle" % awsSdkVersion,
  "com.amazonaws" % "aws-java-sdk-glue" % awsSdkVersion,

  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonDataformatVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" %jacksonDataformatVersion,

  "com.lihaoyi" %% "os-lib" % "0.11.3",
  "com.github.mwiede" % "jsch" % "0.2.22",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)
libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "org.postgresql" % "postgresql" % postgresqlVersion,
  "com.clickhouse" % "clickhouse-jdbc" % clickhouseVersion,
  "io.minio" % "minio" % ioMinioVersion,
  "com.sun.mail" % "javax.mail" % mailVersion,
  "javax.mail" % "javax.mail-api" % mailVersion,
)
libraryDependencies ++= Seq(
  "org.seleniumhq.selenium" % "selenium-java" % "4.28.0",
  "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M25"
)
libraryDependencies ++= Seq(
  "com.google.api-ads" % "google-ads" % googleApiVersion,
)


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDataformatVersion
