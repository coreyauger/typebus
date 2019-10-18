name := "macros"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

version in ThisBuild := "0.0.14-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.5",
  "org.scala-lang" % "scala-compiler" % "2.12.5",
  "com.typesafe.play" %% "play-json" % "2.7.1",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "com.typesafe" % "config" % "1.2.1"
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
