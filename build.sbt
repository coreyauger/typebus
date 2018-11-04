name := "type-bus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.5-SNAPSHOT"


lazy val `type-bus` =
  (project in file("."))
  //.settings(commonSettings:_*)
  //.settings(resolverSettings: _*)

val akkaV = "2.5.14"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "joda-time" % "joda-time" % "2.9.7",
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22"
)

fork in ThisBuild := true
/*

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
*/
