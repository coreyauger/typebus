name := "telemetry"

version := "0.0.1-SNAPSHOT"

organization in ThisBuild := "io.surfkit"

scalaVersion := "2.12.5"

resolvers += Resolver.sonatypeRepo("snapshots")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-language:postfixOps")

lazy val `telemetry` =
  (project in file("."))

val akkaV = "2.5.13"

val squbsV = "0.11.0"

libraryDependencies ++= Seq(
  "org.squbs" %% "squbs-unicomplex" % squbsV,
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.2",
  "io.surfkit" %% "typebus-kafka" % "0.0.5-SNAPSHOT",
  "com.typesafe.akka" %% "akka-cluster-sharding" % akkaV,
  "com.typesafe.akka" %% "akka-persistence-dynamodb" % "1.1.1"
)

mainClass in (Compile, run) := Some("org.squbs.unicomplex.Bootstrap")

