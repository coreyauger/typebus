name := "typebus-root"

organization in ThisBuild := "io.surfkit"

version in ThisBuild := "0.0.5-SNAPSHOT"

publishArtifact := false

lazy val root =
  (project in file("."))
  .aggregate(`typebus`)
  .aggregate(`typebus-kafka`)
  .aggregate(`typebus-testkit`)
  .aggregate(`telemetry`)


val `typebus` = ProjectRef(file("typebus"), "typebus")

val `typebus-kafka` = ProjectRef(file("typebus-kafka"), "typebus-kafka")

val `typebus-testkit` = ProjectRef(file("typebus-testkit"), "typebus-testkit")

val `telemetry` = ProjectRef(file("telemetry/server"), "telemetry")

fork in ThisBuild := true

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

