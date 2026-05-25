import Dependencies.{zio, zioK8s, sttp}

import sbt.Level
import scala.collection.Seq

ThisBuild / version := "0.6.6"

ThisBuild / scalaVersion := "3.8.3"

ThisBuild / organization         := "io.github.tobia80"
ThisBuild / organizationName     := "tobia80"
ThisBuild / organizationHomepage := Some(url("https://tobia80.github.io"))
ThisBuild / maintainer           := "tobia80"

lazy val root = (project in file("."))
  .settings(
    name := "DRef",
    libraryDependencies ++= Seq(
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .enablePlugins(JavaAppPackaging)
  .settings(noPublishSettings)

lazy val noPublishSettings = Seq(publish := (()), publishLocal := (()), publishArtifact := false)

val testDeps = Seq(
  "dev.zio" %% "zio-test"     % zio % Test,
  "dev.zio" %% "zio-test-sbt" % zio % Test
)

val coreDeps = Seq(
  "dev.zio"         %% "zio"                         % zio,
  "dev.zio"         %% "zio-interop-reactivestreams" % "2.0.2",
  "io.github.vigoo" %% "desert-zio"                  % "0.3.6",
  "io.github.vigoo" %% "desert-zio-schema"           % "0.3.6",
  "dev.zio"         %% "zio-schema-msg-pack"         % "1.8.5"
) ++ testDeps

val redisDeps = Seq(
  "io.lettuce" % "lettuce-core" % "7.5.2.RELEASE"
) ++ testDeps

val raftDeps = Seq(
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core" % "0.6.3",
  "org.apache.commons"             % "commons-lang3" % "3.20.0",
  "io.projectreactor"              % "reactor-core"  % "3.8.5",

  // grpc
  "io.grpc"               % "grpc-netty"           % "1.81.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,

  // kubernetes service discovery
  "com.coralogix"                    %% "zio-k8s-client" % zioK8s,
  "com.softwaremill.sttp.client3"    %% "slf4j-backend"  % sttp
) ++ testDeps

lazy val commonProtobufSettings = Seq(
  Compile / PB.targets := Seq(
    scalapb.gen(grpc = true)          -> (Compile / sourceManaged).value,
    scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value
  )
)

def module(id: String, path: String, description: String): Project =
  Project(id, file(path))
    .settings(moduleName := id, name := description)
    .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
    .settings(Test / logLevel := Level.Warn)

lazy val `core` = module("dref-core", "dref-core", "Core library")
  .settings(libraryDependencies ++= coreDeps)

lazy val example = module("example", "example", "Example app").dependsOn(`core`, raft).settings(noPublishSettings)

lazy val interopExample = module("interop-example", "interop-example", "Interop example")
  .enablePlugins(JavaAppPackaging)
  .dependsOn(`core`, raft)
  .settings(noPublishSettings)
  .settings(
    // Without an SLF4J binding on the classpath the JVM defaults to NOP and
    // silently swallows every Raft / gRPC log line, which makes interop
    // failures invisible. slf4j-simple writes to stderr — good enough for a
    // demo container.
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.16"
  )

lazy val redis = module("dref-redis", "dref-redis", "Redis backend")
  .dependsOn(`core`, `core` % "test->test")
  .settings(
    libraryDependencies ++= redisDeps
  )

lazy val raft = module("dref-raft", "dref-raft", "Raft backend")
  .dependsOn(`core`, `core` % "test->test")
  .settings(
    libraryDependencies ++= raftDeps
  )
  .settings(commonProtobufSettings)
  .settings(
    Compile / PB.protoSources := Seq(baseDirectory.value.getParentFile / "proto")
  )

aggregateProjects(`core`, redis, raft, example, interopExample)

// NativePackager settings
enablePlugins(UniversalPlugin)

publishTo := sonatypePublishToBundle.value
