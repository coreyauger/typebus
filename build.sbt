name := "typebus-root"

organization in ThisBuild := "io.surfkit"

version in ThisBuild := "0.0.1-SNAPSHOT"

publishArtifact := false

lazy val root =
  (project in file("."))
  .aggregate(`typebus`)
  .aggregate(`typebus-kafka`)
  .aggregate(`typebus-kinesis`)


val `typebus` = ProjectRef(file("typebus"), "typebus")

val `typebus-kafka` = ProjectRef(file("typebus-kafka"), "typebus-kafka")

val `typebus-kinesis` = ProjectRef(uri("typebus-kinesis"), "typebus-kinesis")

fork in ThisBuild := true

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

