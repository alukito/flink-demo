FROM maven:3.9.9-eclipse-temurin-11 AS builder
WORKDIR /build
COPY flink/pom.xml ./pom.xml
RUN mvn -B dependency:go-offline
COPY flink/src ./src
RUN mvn -B clean verify

FROM flink:1.19.3-scala_2.12-java11
COPY --from=builder /build/target/level2-jobs.jar /opt/flink/usrlib/level2-jobs.jar
COPY flink/submit-jobs.sh /opt/flink/submit-jobs.sh
ENTRYPOINT ["/opt/flink/submit-jobs.sh"]
