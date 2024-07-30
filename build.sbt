import sbt.Keys.libraryDependencies
import scala.collection.Seq


ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "etl-tool"
  )

val sparkVersion = "3.4.3"
val hadoopVersion = "3.4.0"
val jacksonDataformatVersion = "2.14.3"
val json4sVersion = "4.0.7"
val awsSdk = "1.12.765"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDataformatVersion,
).map(_.cross(CrossVersion.for3Use2_13))

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "mysql" % "mysql-connector-java" % "8.0.33",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "joda-time" % "joda-time" % "2.12.7",
  "io.minio" % "minio" % "8.5.10",
  "com.amazonaws" % "aws-java-sdk" % awsSdk,
  "com.amazonaws" % "aws-java-sdk-s3" % awsSdk,
  "com.amazonaws" % "aws-java-sdk-bundle" % awsSdk,
  "com.amazonaws" % "aws-java-sdk-glue" % awsSdk,

  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonDataformatVersion,

  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" %jacksonDataformatVersion ,
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDataformatVersion
