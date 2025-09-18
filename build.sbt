import Dependencies.zio

import scala.collection.Seq

ThisBuild / version := "0.4.1"

ThisBuild / scalaVersion := "3.5.2"

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
  "io.github.vigoo" %% "desert-zio-schema"           % "0.3.6"
) ++ testDeps

val redisDeps = Seq(
  "io.lettuce" % "lettuce-core" % "6.8.1.RELEASE"
) ++ testDeps

val raftDeps = Seq(
  "io.microraft"                   % "microraft"     % "0.7",
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core" % "0.6.3",
  "org.apache.commons"             % "commons-lang3" % "3.18.0",
  "io.projectreactor"              % "reactor-core"  % "3.7.11",

  // grpc
  "io.grpc"               % "grpc-netty"           % "1.75.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
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

lazy val `core` = module("dref-core", "dref-core", "Core library")
  .settings(libraryDependencies ++= coreDeps)

lazy val example = module("example", "example", "Example app").dependsOn(`core`, raft).settings(noPublishSettings)

lazy val redis = module("dref-redis", "dref-redis", "Redis backend")
  .dependsOn(`core`)
  .settings(
    libraryDependencies ++= redisDeps
  )

lazy val raft = module("dref-raft", "dref-raft", "Raft backend")
  .dependsOn(`core`)
  .settings(
    libraryDependencies ++= raftDeps
  )
  .settings(commonProtobufSettings)

aggregateProjects(`core`, redis, raft, example)

// NativePackager settings
enablePlugins(UniversalPlugin)

publishTo := sonatypePublishToBundle.value
