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
      "dev.zio"         %% "zio"                % "2.1.20",
      "io.github.vigoo" %% "desert-zio"         % "0.3.6",
      "io.github.vigoo" %% "desert-zio-schema"  % "0.3.6",
      "dev.profunktor"  %% "redis4cats-effects" % "1.7.2",
      "dev.profunktor"  %% "redis4cats-streams" % "1.7.2",
      "dev.zio"         %% "zio-interop-cats"   % "23.1.0.5",
      // test
      "dev.zio"         %% "zio-test"           % "2.1.20" % Test,
      "dev.zio"         %% "zio-test-sbt"       % "2.1.20" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .enablePlugins(JavaAppPackaging)

// NativePackager settings
enablePlugins(UniversalPlugin)
