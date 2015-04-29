organization := "org.labrad"

name := "scalabrad"

version := "0.2.0-M6"

scalaVersion := "2.11.6"

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
  "org.scala-lang.modules" %% "scala-swing" % "1.0.1",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1",
  "org.clapper" %% "argot" % "1.0.3",
  "io.netty" % "netty-all" % "5.0.0.Alpha2",
  "joda-time" % "joda-time" % "2.1",
  "org.joda" % "joda-convert" % "1.2",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "ch.qos.logback" % "logback-classic" % "1.0.6",
  "com.typesafe.play" %% "anorm" % "2.4.0-M2",
  "org.xerial" % "sqlite-jdbc" % "3.8.7"
)


// testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

parallelExecution in Test := false


// use bintray to publish library jars
bintraySettings


// use sbt-pack to create distributable package
packSettings

packMain := Map(
  "labrad" -> "org.labrad.manager.Manager",
  "labrad-migrate-registry" -> "org.labrad.registry.Migrate",
  "labrad-sql-test" -> "org.labrad.registry.SQLTest"
)

packGenerateWindowsBatFile := true
