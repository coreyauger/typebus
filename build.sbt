name := "typebus-root"

organization in ThisBuild := "io.surfkit"

version in ThisBuild := "0.0.13-SNAPSHOT"

publishArtifact := false

lazy val root =
  (project in file("."))
  .aggregate(`typebus`)
  .aggregate(`typebus-kafka`)
  .aggregate(`typebus-akka`)
  .aggregate(`typebus-testkit`)


val `typebus` = ProjectRef(file("typebus"), "typebus")

val `typebus-kafka` = ProjectRef(file("typebus-kafka"), "typebus-kafka")

val `typebus-akka` = ProjectRef(file("typebus-akka"), "typebus-akka")

val `typebus-testkit` = ProjectRef(file("typebus-testkit"), "typebus-testkit")

fork in ThisBuild := true

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)


