organization := "org.labrad"

name := "scalabrad"

version := "0.2-SNAPSHOT"

scalaVersion := "2.11.2"

scalacOptions ++= Seq(
  "-deprecation",
  "-target:jvm-1.6",
  "-feature"
  // "-Ymacro-debug-lite"
)

EclipseKeys.withSource := true

EclipseKeys.eclipseOutput := Some("target/eclipseOutput")

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += Resolver.sonatypeRepo("releases")

//addCompilerPlugin("org.scala-lang.plugins" % "macro-paradise_2.11.1" % "2.0.0-SNAPSHOT")


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-actors" % scalaVersion.value,
  "org.scala-lang.modules" %% "scala-swing" % "1.0.1",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1"
)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.9.5.Final" withSources(),
  "joda-time" % "joda-time" % "2.1" withSources(),
  "org.joda" % "joda-convert" % "1.2" withSources(),
  "org.slf4j" % "slf4j-api" % "1.7.2"
)

publishArtifact in (Compile, packageDoc) := false


// logging
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.6" withSources()


// testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)

parallelExecution in Test := false

