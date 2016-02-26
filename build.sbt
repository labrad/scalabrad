organization := "org.labrad"

name := "scalabrad"

version := "0.5.4"

scalaVersion := "2.11.7"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature"
)

EclipseKeys.withSource := true

EclipseKeys.eclipseOutput := Some("target/eclipseOutput")

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += Resolver.sonatypeRepo("releases")

licenses += ("GPL-2.0", url("http://www.gnu.org/licenses/gpl-2.0.html"))


// dependencies
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1",
  "org.clapper" %% "argot" % "1.0.4",
  "io.netty" % "netty-all" % "4.1.0.Beta8",
  "joda-time" % "joda-time" % "2.1",
  "org.joda" % "joda-convert" % "1.2",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "ch.qos.logback" % "logback-classic" % "1.0.6",
  "com.typesafe.play" %% "anorm" % "2.4.0-M2",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.52",
  "org.bouncycastle" % "bcpkix-jdk15on" % "1.52"
)

// When running, connect std in and tell manager to stop on EOF (ctrl+D).
// This allows us to stop the manager without using ctrl+C, which kills sbt.
fork in run := true
connectInput in run := true
javaOptions += "-Dorg.labrad.stopOnEOF=true"


// testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

fork in Test := true
parallelExecution in Test := false


// use bintray to publish library jars
bintrayOrganization := Some("labrad")
bintrayReleaseOnPublish in ThisBuild := false


// use sbt-pack to create distributable package
packSettings

packMain := Map(
  "labrad" -> "org.labrad.manager.Manager",
  "labrad-migrate-registry" -> "org.labrad.registry.Migrate",
  "labrad-sql-test" -> "org.labrad.registry.SQLTest"
)

packGenerateWindowsBatFile := true
