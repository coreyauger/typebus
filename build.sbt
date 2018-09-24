name := "type-bus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.4-SNAPSHOT"


lazy val `type-bus` =
  (project in file("."))
  //.settings(commonSettings:_*)
  //.settings(resolverSettings: _*)

val akkaV = "2.5.14"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-stream" % akkaV,
  "com.typesafe.akka" %% "akka-cluster" % akkaV,
  "joda-time" % "joda-time" % "2.9.7",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.13"
)

fork in ThisBuild := true
/*

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
*/
