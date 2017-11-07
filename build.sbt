name := "type-bus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.11.8"

version in ThisBuild := "0.0.4-SNAPSHOT"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

resolvers += "NextWave Repo" at "https://repository.conversant.im/artifactory/nxtwv-maven/"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

lazy val `type-bus` =
  (project in file("."))
  .settings(publishTo := Some("NextWave Repo" at "https://repository.conversant.im/artifactory/nxtwv-maven/"), publishArtifact in (Compile, packageDoc) := false)
  //.settings(commonSettings:_*)
  //.settings(resolverSettings: _*)

val akkaV = "2.4.14"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-stream" % akkaV,
  "com.typesafe.akka" %% "akka-cluster" % akkaV,
  "org.scala-lang" % "scala-compiler" % "2.11.8",
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
