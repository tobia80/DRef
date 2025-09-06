addDependencyTreePlugin

addSbtPlugin("com.thesamet"   % "sbt-protoc"          % "1.0.8")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"        % "2.5.2")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"        % "0.13.0")

libraryDependencies +=
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"
