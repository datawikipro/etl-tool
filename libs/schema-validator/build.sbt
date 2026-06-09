name := "schema-validator"

organization := "pro.datawiki"

version := "0.1.0-SNAPSHOT"

scalaVersion := "3.4.2"

val sparkVersion = "3.5.6"
val jacksonDataformatVersion = "2.18.2"
val json4sVersion = "3.7.0-M11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll (
    ExclusionRule(organization = "org.scala-lang.modules", name = "scala-xml_2.13")
  ),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll (
    ExclusionRule(organization = "org.scala-lang.modules", name = "scala-xml_2.13")
  ),
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDataformatVersion,
).map(_.cross(CrossVersion.for3Use2_13))

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonDataformatVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonDataformatVersion,
  "org.slf4j" % "slf4j-api" % "2.0.12"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDataformatVersion,
  "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
)
