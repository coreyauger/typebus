name := "macros"

organization in ThisBuild := "io.surfkit"

scalaVersion in ThisBuild := "2.12.5"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.5",
  "org.scala-lang" % "scala-compiler" % "2.12.5",
  "io.suzaku" %% "boopickle" % "1.3.0"
)

val paradiseVersion = "2.1.1"

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)

/*

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
*/
