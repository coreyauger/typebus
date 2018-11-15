name := "typebus-kinesis"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.5-SNAPSHOT"


lazy val `typebus-kinesis` =
  (project in file("."))
  //.settings(commonSettings:_*)
  //.settings(resolverSettings: _*)

val akkaV = "2.5.13"

libraryDependencies ++= Seq(
  "io.surfkit" %% "typebus" %  "0.0.5-SNAPSHOT",
  "com.typesafe.akka"       %% "akka-cluster" % akkaV,
  "com.typesafe.akka"       %% "akka-cluster-tools" % akkaV,
  "com.lightbend.akka" %% "akka-stream-alpakka-kinesis" % "1.0-M1"
)

fork in ThisBuild := true

/*

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
*/
