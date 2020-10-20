FROM hseeberger/scala-sbt:11.0.8_1.4.0_2.12.12 as builder

WORKDIR /tmp/clin-pipelines
COPY . .

RUN sbt clean assembly

FROM openjdk:11

COPY --from=builder /tmp/clin-pipelines/target/scala-2.12/clin-pipelines.jar .

ENTRYPOINT ["java", "-jar", "clin-pipelines.jar"]