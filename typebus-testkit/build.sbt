name := "typebus-testkit"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

val ver = "0.0.6-SNAPSHOT"

version in ThisBuild := ver

lazy val `typebus-testkit` =
  (project in file("."))

val akkaV = "2.5.13"

libraryDependencies ++= Seq(
  "io.surfkit"        %% "typebus" %  ver,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaV
)

fork in ThisBuild := true
