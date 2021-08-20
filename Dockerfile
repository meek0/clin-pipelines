FROM openjdk:11

COPY target/scala-2.12/clin-pipelines.jar .

ENTRYPOINT ["java", "-cp", "clin-pipelines.jar"]