name := "type-bus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.11.8"

version in ThisBuild := "0.0.3-SNAPSHOT"

resolvers += "NextWave Repo" at "http://maxdevmaster.cloudapp.net:4343/artifactory/nxtwv-maven/"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := Some("NextWave Repo" at "http://maxdevmaster.cloudapp.net:4343/artifactory/nxtwv-maven/")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

lazy val `type-bus` =
  (project in file("."))
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
