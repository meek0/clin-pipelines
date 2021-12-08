
name := "clin-pipelines"

version := "0.1"

scalaVersion := "2.12.12"
scalacOptions += "-Ypartial-unification"

val awssdkVersion = "2.16.66"
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % awssdkVersion,
  "software.amazon.awssdk" % "apache-client" % awssdkVersion,
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "ca.uhn.hapi.fhir" % "hapi-fhir-client" % "5.0.2",
  "ca.uhn.hapi.fhir" % "hapi-fhir-structures-r4" % "5.0.2",
  "ca.uhn.hapi.fhir" % "org.hl7.fhir.r4" % "5.0.0",
  "org.typelevel" %% "cats-core" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",
  "org.keycloak" % "keycloak-authz-client" % "12.0.3",
  "com.softwaremill.sttp.client3" %% "core" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.38.8" % "test",
  "org.testcontainers" % "localstack" % "1.15.2" %"test",
  "com.typesafe.play" %% "play-mailer" % "8.0.1" ,
  "com.typesafe.play" %% "play-mailer-guice" % "8.0.1"
)

Test / fork := true
Test / testForkedParallel := false

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assembly / test := {}
parallelExecution / test := false
assembly / assemblyJarName:= "clin-pipelines.jar"
