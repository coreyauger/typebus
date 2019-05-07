name := "typebus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.6-SNAPSHOT"

lazy val macros = project

lazy val `typebus` =
  (project in file("."))
    .aggregate(macros)
    .dependsOn(macros)

val akkaV = "2.5.13"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-cluster" % akkaV,
  "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.2"
)

val paradiseVersion = "2.1.1"

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)

