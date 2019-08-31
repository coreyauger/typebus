name := "typebus"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.8-SNAPSHOT"

lazy val macros = project

lazy val `typebus` =
  (project in file("."))
    .aggregate(macros)
    .dependsOn(macros)

val akkaV = "2.5.23"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaV,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaV,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaV,
  "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.2"
)

val paradiseVersion = "2.1.1"

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)

// POM settings for Sonatype
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
homepage := Some(url("https://github.com/coreyauger/typebus"))
scmInfo := Some(ScmInfo(url("https://github.com/coreyauger/typebus"), "git@github.com:coreyauger/typebus.git"))
developers := List(Developer("coreyauger",
  "Corey Auger",
  "coreyauger@gmail.com",
  url("https://github.com/coreyauger")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ => false }
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
publishMavenStyle := true
