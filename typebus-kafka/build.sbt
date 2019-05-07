name := "typebus-kafka"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

val ver = "0.0.6-SNAPSHOT"

version in ThisBuild := ver

lazy val `typebus-kafka` =
  (project in file("."))

val akkaV = "2.5.13"

libraryDependencies ++= Seq(
  "io.surfkit"        %% "typebus" %  ver,
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0-RC1"
)

fork in ThisBuild := true
