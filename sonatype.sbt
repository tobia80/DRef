import xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / organization         := "io.github.tobia80"
ThisBuild / organizationName     := "Tobia80"
ThisBuild / organizationHomepage := Some(url("https://tobia80.github.io"))

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

ThisBuild / scmInfo                := Some(
  ScmInfo(
    url("https://github.com/tobia80/dref"),
    "scm:git@github.com:tobia80/dref.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "tobia80",
    name = "Tobia Loschiavo",
    email = "tobia.loschiavo@gmail.com",
    url = url("https://tobia80.github.io")
  )
)

ThisBuild / description := "DRef, (Distributed Ref) is a distributed variable implementation designed to synchronize state across multiple nodes"
ThisBuild / licenses    := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage    := Some(url("https://github.com/tobia80/dref"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

ThisBuild / publishTo         := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else sonatypePublishToBundle.value
}
ThisBuild / publishMavenStyle := true
