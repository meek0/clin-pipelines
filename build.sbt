name := "clin-pipelines"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-bom" % "1.11.880",
  "com.amazonaws" % "aws-java-sdk-s3"  % "1.11.880",
  "org.slf4j" % "slf4j-api" % "1.7.30",
  "org.slf4j" % "slf4j-simple" % "1.7.30"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "clin-pipelines.jar"