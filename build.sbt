
name := "clin-pipelines"

version := "0.1"

scalaVersion := "2.12.12"
scalacOptions += "-Ypartial-unification"

val awssdkVersion = "2.24.7"
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % awssdkVersion,
  "software.amazon.awssdk" % "apache-client" % awssdkVersion,
  "software.amazon.awssdk" % "s3-transfer-manager" % awssdkVersion,
  "software.amazon.awssdk.crt" % "aws-crt" % "0.29.10",
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "ca.uhn.hapi.fhir" % "hapi-fhir-client" % "5.4.2",
  "ca.uhn.hapi.fhir" % "hapi-fhir-structures-r4" % "5.4.2",
  "ca.uhn.hapi.fhir" % "org.hl7.fhir.r4" % "5.0.0",
  "org.json" % "json" % "20210307",
  "org.typelevel" %% "cats-core" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",
  "org.keycloak" % "keycloak-authz-client" % "12.0.3",
  "com.softwaremill.sttp.client3" %% "core" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.17" % Test,
  "com.dimafeng" %% "testcontainers-scala-localstack" % "0.40.17" % Test,
  "com.typesafe.play" %% "play-mailer" % "8.0.1"
)
excludeDependencies ++= Seq(
  ExclusionRule("com.sun.activation", "jakarta.activation"),
  ExclusionRule("commons-logging", "commons-logging"), // commons-logging is replaced by jcl-over-slf4j
)
Test / fork := true
Test / testForkedParallel := false

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", "mailcap") => MergeStrategy.first
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case x => MergeStrategy.defaultMergeStrategy(x)
}
assembly / test := {}
parallelExecution / test := false
assembly / assemblyJarName:= "clin-pipelines.jar"
