# syntax=docker/dockerfile:1.6

FROM sbtscala/scala-sbt:eclipse-temurin-21.0.5_11_1.10.8_3.5.1 AS build
WORKDIR /app
COPY project ./project
COPY build.sbt .
COPY sonatype.sbt .
COPY example ./example
COPY dref-core ./dref-core
COPY dref-raft ./dref-raft
COPY dref-redis ./dref-redis
COPY README.md ./
COPY LICENSE ./
RUN sbt example/stage

FROM eclipse-temurin:21-jre
WORKDIR /opt/dref
COPY --from=build /app/example/target/universal/stage/ ./
ENV JAVA_OPTS="-Xms512m -Xmx512m"
EXPOSE 8082
ENTRYPOINT ["bin/example"]
