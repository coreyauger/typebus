name := "typebus-testkit"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

val ver = "0.0.7-SNAPSHOT"

version in ThisBuild := ver

lazy val `typebus-testkit` =
  (project in file("."))

val akkaV = "2.5.23"

libraryDependencies ++= Seq(
  "io.surfkit"        %% "typebus" %  ver,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaV
)

fork in ThisBuild := true


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

