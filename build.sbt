ThisBuild / organization := "org.labrad"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

lazy val commonSettings = Seq(
  version := {
    IO.read(file("core/src/main/resources/org/labrad/version.txt")).trim()
  },

  scalacOptions ++= Seq(
    "-deprecation",
    "-feature"
  ),

  licenses += ("GPL-2.0", url("http://www.gnu.org/licenses/gpl-2.0.html")),

  // dependencies
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.lihaoyi" %% "fastparse" % "0.4.4",
    "io.netty" % "netty-all" % "4.1.82.Final",
    "joda-time" % "joda-time" % "2.1",
    "org.joda" % "joda-convert" % "1.2",
    "org.slf4j" % "slf4j-api" % "1.7.2",
    "ch.qos.logback" % "logback-classic" % "1.0.6",
    "org.playframework.anorm" %% "anorm" % "2.7.0",
    "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.52",
    "org.bouncycastle" % "bcpkix-jdk15on" % "1.52",
    "org.mindrot" % "jbcrypt" % "0.3m",
    "com.google.api-client" % "google-api-client" % "1.19.0",
    "com.google.http-client" % "google-http-client" % "1.19.0"
  ),

  // When running, connect std in and tell manager to stop on EOF (ctrl+D).
  // This allows us to stop the manager without using ctrl+C, which kills sbt.
  run / fork := true,
  run / connectInput := true,
  javaOptions += "-Dorg.labrad.stopOnEOF=true",

  // testing
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  ),

  Test / fork := true,
  Test / parallelExecution := false,
  Test / javaOptions += "-Xmx1g",
)

lazy val all = project.in(file("."))
  .aggregate(argot, core, manager)

lazy val argot = project.in(file("argot"))
  .settings(
    libraryDependencies ++= Seq(
      "org.clapper" %% "grizzled-scala" % "4.10.0",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    )
  )

lazy val core = project.in(file("core"))
  .dependsOn(argot)
  .settings(commonSettings)
  .settings(
    name := "scalabrad-core"
  )

lazy val manager = project.in(file("manager"))
  .dependsOn(core)
  .enablePlugins(PackPlugin)
  .settings(commonSettings)
  .settings(
    name := "scalabrad-manager",

    packMain := Map(
      "labrad" -> "org.labrad.manager.Manager",
      "labrad-migrate-registry" -> "org.labrad.registry.Migrate",
      "labrad-sql-test" -> "org.labrad.registry.SQLTest"
    ),
    packGenerateWindowsBatFile := true,
    packArchivePrefix := "scalabrad"
  )
