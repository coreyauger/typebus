name := "typebus-squbs"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.5-SNAPSHOT"

lazy val `typebus-squbs` =
  (project in file("."))

val akkaV = "2.5.13"

val squbsV = "0.11.0"

libraryDependencies ++= Seq(
  "org.squbs"               %% "squbs-unicomplex" % squbsV,
  "com.typesafe.akka"       %% "akka-cluster" % akkaV,
  "com.typesafe.akka"       %% "akka-cluster-tools" % akkaV,
  "com.typesafe.akka"       %% "akka-cluster-sharding" % akkaV,
  "com.typesafe.akka"       %% "akka-persistence" % akkaV,
  "com.typesafe.akka"       %% "akka-persistence-cassandra" % "0.91",
  "com.sclasen"             %% "akka-zk-cluster-seed" % "0.1.10"
)

fork in ThisBuild := true

