import scala.collection.Seq

ThisBuild / version := "0.2.1"

ThisBuild / scalaVersion := "3.5.2"

ThisBuild / organization         := "io.github.tobia80"
ThisBuild / organizationName     := "tobia80"
ThisBuild / organizationHomepage := Some(url("https://tobia80.github.io"))

lazy val root = (project in file("."))
  .settings(
    name := "Cref",
    libraryDependencies ++= Seq(
      "dev.zio"         %% "zio"                         % "2.1.21",
      "dev.zio"         %% "zio-interop-reactivestreams" % "2.0.2",
      "io.github.vigoo" %% "desert-zio"                  % "0.3.6",
      "io.github.vigoo" %% "desert-zio-schema"           % "0.3.6",

      // redis
      "io.lettuce" % "lettuce-core" % "6.8.1.RELEASE",

      // raft
      "io.microraft"                   % "microraft"     % "0.7",
      "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core" % "0.6.3",
      "org.apache.commons"             % "commons-lang3" % "3.18.0",

      // test
      "dev.zio" %% "zio-test"     % "2.1.21" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.1.21" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .enablePlugins(JavaAppPackaging)

Compile / PB.targets := Seq(
  scalapb.gen(grpc = true)          -> (Compile / sourceManaged).value,
  scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value
)

libraryDependencies ++= Seq(
  "io.grpc"               % "grpc-netty"           % "1.75.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

// NativePackager settings
enablePlugins(UniversalPlugin)
