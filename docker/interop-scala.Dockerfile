# syntax=docker/dockerfile:1.6
# Scala node for the cross-language DRef interop demo.
# Used by docker-compose.interop.yml together with docker/interop-rust.Dockerfile.

FROM sbtscala/scala-sbt:eclipse-temurin-21.0.8_9_1.12.11_3.8.3 AS build
WORKDIR /app

# Project + dependency definitions first to maximise layer caching.
COPY project ./project
COPY build.sbt .
COPY sonatype.sbt .

# Sources required to compile the interop-example module.
COPY dref-core ./dref-core
COPY dref-raft ./dref-raft
COPY dref-redis ./dref-redis
COPY example ./example
COPY interop-example ./interop-example
COPY proto ./proto
COPY README.md ./
COPY LICENSE ./

ENV SBT_OPTS="-Xms512m -Xmx3g -XX:ReservedCodeCacheSize=256m -XX:MaxMetaspaceSize=512m"
RUN sbt "interop-example/stage"

FROM eclipse-temurin:21-jre
WORKDIR /opt/dref
COPY --from=build /app/interop-example/target/universal/stage/ ./
ENV JAVA_OPTS="-Xms256m -Xmx512m" \
    DREF_PORT=8082
EXPOSE 8082
ENTRYPOINT ["bin/interop-example"]
