name := "clin-pipelines"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-bom" % "1.11.880",
  "com.amazonaws" % "aws-java-sdk-s3"  % "1.11.880",
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "ca.uhn.hapi.bio.ferlab.clin.etl.fhir" % "hapi-bio.ferlab.clin.etl.fhir-client" % "5.0.2",
  "ca.uhn.hapi.bio.ferlab.clin.etl.fhir" % "hapi-bio.ferlab.clin.etl.fhir-structures-r4" % "5.0.2",
  "ca.uhn.hapi.bio.ferlab.clin.etl.fhir" % "org.hl7.bio.ferlab.clin.etl.fhir.r4" % "5.0.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.1.0",
  "com.google.code.gson" % "gson" % "2.8.6",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.38.5" % "test"
)

Test / fork := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "clin-pipelines.jar"
